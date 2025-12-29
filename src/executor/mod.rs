use polars::prelude::*;
use regex::Regex;
use std::collections::HashMap;

use crate::error::{DtransformError, Result};
use crate::parser::ast::*;

pub struct Executor {
    variables: HashMap<String, DataFrame>,
}

/// Auto-detect delimiter from file content
/// Returns (delimiter, needs_trim_whitespace)
fn auto_detect_delimiter(content: &str, file_extension: Option<&str>) -> Result<(char, bool)> {
    // For .tsv files, use tab delimiter but check if trimming is needed
    if file_extension == Some("tsv") {
        let needs_trim = content.lines().take(100).any(|line| {
            line.trim() != line || line.contains("  ")
        });
        return Ok(('\t', needs_trim));
    }

    // For .csv files, prefer comma but check if trimming is needed
    if file_extension == Some("csv") {
        // Check if file has leading/trailing whitespace or multiple consecutive spaces
        let needs_trim = content.lines().take(100).any(|line| {
            line.trim() != line || line.contains("  ")
        });
        return Ok((',', needs_trim));
    }

    // Sample first 100 non-empty lines for detection
    let sample_lines: Vec<&str> = content
        .lines()
        .filter(|l| !l.trim().is_empty())
        .take(100)
        .collect();

    if sample_lines.is_empty() {
        return Err(DtransformError::InvalidOperation(
            "File is empty or contains no data".to_string()
        ));
    }

    // Check if trimming whitespace is needed
    let needs_trim = sample_lines.iter().any(|line| {
        line.trim() != *line || line.contains("  ")
    });

    // Prepare content for delimiter detection
    let detection_lines: Vec<String> = if needs_trim {
        sample_lines.iter().map(|line| {
            let trimmed = line.trim();
            trimmed.split_whitespace().collect::<Vec<_>>().join(" ")
        }).collect()
    } else {
        sample_lines.iter().map(|s| s.to_string()).collect()
    };

    // Count occurrences of common delimiters
    let delimiters = [',', '\t', '|', ';', ' '];
    let mut delimiter_counts: HashMap<char, Vec<usize>> = HashMap::new();

    for line in &detection_lines {
        for &delim in &delimiters {
            let count = line.matches(delim).count();
            delimiter_counts.entry(delim).or_insert_with(Vec::new).push(count);
        }
    }

    // Find delimiter with:
    // 1. Non-zero consistent counts across lines
    // 2. Highest average count
    let mut best_delimiter = None;
    let mut best_score = 0.0;

    for (&delim, counts) in &delimiter_counts {
        // Filter out lines with zero occurrences
        let non_zero_counts: Vec<usize> = counts.iter().filter(|&&c| c > 0).copied().collect();

        if non_zero_counts.is_empty() {
            continue;
        }

        // Check consistency: most lines should have the same count
        let min = *non_zero_counts.iter().min().unwrap();
        let max = *non_zero_counts.iter().max().unwrap();
        let avg = non_zero_counts.iter().sum::<usize>() as f64 / non_zero_counts.len() as f64;

        // Delimiter should appear consistently (variance should be low)
        // and appear in most lines
        let consistency = non_zero_counts.len() as f64 / detection_lines.len() as f64;

        // Prefer delimiters that appear consistently
        if min == max || (max as f64 - min as f64) / avg < 0.3 {
            let score = avg * consistency;
            if score > best_score {
                best_score = score;
                best_delimiter = Some(delim);
            }
        }
    }

    match best_delimiter {
        Some(delim) => Ok((delim, needs_trim)),
        None => {
            // No delimiter found - likely a single-column file
            // Check if ALL delimiter counts are zero across all lines
            let all_zero = delimiter_counts.values().all(|counts| {
                counts.iter().all(|&c| c == 0)
            });

            if all_zero {
                // Single column file - use comma as default (won't matter since no delimiters)
                Ok((',', needs_trim))
            } else {
                // Ambiguous format - multiple delimiters present but inconsistent
                Err(DtransformError::InvalidOperation(
                    "Could not auto-detect delimiter. The file format is ambiguous.\n\n\
                    Please specify the delimiter explicitly:\n\
                    • Comma: read('file', delimiter=',')\n\
                    • Tab: read('file', delimiter='\\t')\n\
                    • Pipe: read('file', delimiter='|')\n\
                    • Semicolon: read('file', delimiter=';')\n\
                    • Space: read('file', delimiter=' ')".to_string()
                ))
            }
        }
    }
}

impl Executor {
    pub fn new() -> Self {
        Self {
            variables: HashMap::new(),
        }
    }

    pub fn execute_program(&mut self, program: Program) -> Result<Option<DataFrame>> {
        let mut last_result = None;

        for statement in program.statements {
            match statement {
                Statement::Assignment { name, pipeline } => {
                    let df = self.execute_pipeline(pipeline)?;
                    self.variables.insert(name, df);
                    // Assignments don't produce output in program mode
                }
                Statement::Pipeline(pipeline) => {
                    let df = self.execute_pipeline(pipeline)?;
                    last_result = Some(df);
                }
            }
        }

        Ok(last_result)
    }

    pub fn execute_statement(&mut self, statement: Statement) -> Result<Option<DataFrame>> {
        match statement {
            Statement::Assignment { name, pipeline } => {
                let df = self.execute_pipeline(pipeline)?;
                self.variables.insert(name.clone(), df.clone());
                Ok(Some(df))
            }
            Statement::Pipeline(pipeline) => {
                let df = self.execute_pipeline(pipeline)?;
                Ok(Some(df))
            }
        }
    }

    pub fn execute_pipeline(&mut self, pipeline: Pipeline) -> Result<DataFrame> {
        let mut df = match pipeline.source {
            Some(Source::Read(read_op)) => self.execute_read(read_op)?,
            Some(Source::Variable(var_name)) => {
                self.variables
                    .get(&var_name)
                    .ok_or_else(|| DtransformError::VariableNotFound(var_name.clone()))?
                    .clone()
            }
            None => {
                return Err(DtransformError::InvalidOperation(
                    "Pipeline must start with a data source (read() or variable)".to_string(),
                ));
            }
        };

        for operation in pipeline.operations {
            df = self.execute_operation(df, operation)?;
        }

        Ok(df)
    }

    fn execute_operation(&mut self, df: DataFrame, op: Operation) -> Result<DataFrame> {
        match op {
            Operation::Read(read_op) => self.execute_read(read_op),
            Operation::Variable(_var_name) => {
                // Variable references should be handled as pipeline sources, not operations
                Err(DtransformError::InvalidOperation(
                    "Variable references can only be used as pipeline sources, not as operations".to_string()
                ))
            }
            Operation::Write(write_op) => self.execute_write(df, write_op),
            Operation::Select(select_op) => self.execute_select(df, select_op),
            Operation::Filter(filter_op) => self.execute_filter(df, filter_op),
            Operation::Mutate(mutate_op) => self.execute_mutate(df, mutate_op),
            Operation::Rename(rename_op) => self.execute_rename(df, rename_op),
            Operation::RenameAll(rename_all_op) => self.execute_rename_all(df, rename_all_op),
            Operation::Sort(sort_op) => self.execute_sort(df, sort_op),
            Operation::Take(take_op) => self.execute_take(df, take_op),
            Operation::Skip(skip_op) => self.execute_skip(df, skip_op),
            Operation::Slice(slice_op) => self.execute_slice(df, slice_op),
            Operation::Drop(drop_op) => self.execute_drop(df, drop_op),
            Operation::Distinct(distinct_op) => self.execute_distinct(df, distinct_op),
        }
    }

    fn check_duplicate_columns(&self, df: &DataFrame) -> Result<()> {
        use std::collections::HashSet;
        let column_names: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();
        let mut seen = HashSet::new();
        let mut duplicates = Vec::new();

        for name in &column_names {
            if !seen.insert(name) {
                duplicates.push(name.clone());
            }
        }

        if !duplicates.is_empty() {
            return Err(DtransformError::InvalidOperation(format!(
                "File contains duplicate column names: {}. Malformed files with repeated columns are not allowed.",
                duplicates.join(", ")
            )));
        }

        Ok(())
    }

    fn execute_read(&self, op: ReadOp) -> Result<DataFrame> {
        let path = std::path::Path::new(&op.path);

        // Determine format from extension or explicit format
        let format = op.format.as_deref().or_else(|| path.extension()?.to_str());

        match format {
            Some("csv") | Some("tsv") | None => {
                let has_header = op.header.unwrap_or(true);
                let skip_rows = op.skip_rows.unwrap_or(0);

                // Determine delimiter and trim_whitespace
                let (delimiter, trim_whitespace) = if op.delimiter.is_none() || op.trim_whitespace.is_none() {
                    // Need to auto-detect delimiter and/or trim_whitespace
                    let content = std::fs::read_to_string(path)?;
                    let (detected_delim, detected_trim) = auto_detect_delimiter(&content, format)?;

                    (
                        op.delimiter.unwrap_or(detected_delim),
                        op.trim_whitespace.unwrap_or(detected_trim)
                    )
                } else {
                    (op.delimiter.unwrap(), op.trim_whitespace.unwrap())
                };

                let result = if trim_whitespace {
                    // Read file, trim each line, and collapse multiple spaces
                    let content = std::fs::read_to_string(path)?;
                    let trimmed_content: String = content
                        .lines()
                        .map(|line| {
                            // Trim leading/trailing whitespace
                            let trimmed = line.trim();
                            // Collapse multiple whitespace into single space
                            trimmed.split_whitespace().collect::<Vec<_>>().join(" ")
                        })
                        .collect::<Vec<_>>()
                        .join("\n");

                    let cursor = std::io::Cursor::new(trimmed_content.as_bytes());
                    CsvReadOptions::default()
                        .with_has_header(has_header)
                        .with_skip_rows(skip_rows)
                        .with_parse_options(
                            CsvParseOptions::default()
                                .with_separator(delimiter as u8)
                        )
                        .into_reader_with_file_handle(cursor)
                        .finish()
                } else {
                    // Standard file path reading
                    CsvReadOptions::default()
                        .with_has_header(has_header)
                        .with_skip_rows(skip_rows)
                        .with_parse_options(
                            CsvParseOptions::default()
                                .with_separator(delimiter as u8)
                        )
                        .try_into_reader_with_file_path(Some(path.into()))?
                        .finish()
                };

                match result {
                    Ok(df) => {
                        self.check_duplicate_columns(&df)?;
                        Ok(df)
                    },
                    Err(e) => {
                        let error_msg = e.to_string();
                        if error_msg.contains("found more fields") || error_msg.contains("Schema") {
                            Err(DtransformError::InvalidOperation(
                                format!(
                                    "CSV parsing error: Rows have different numbers of fields.\n\n\
                                    The auto-detected settings may be incorrect:\n\
                                    • Detected delimiter: {:?}\n\
                                    • Detected trim_whitespace: {}\n\n\
                                    Try specifying explicitly:\n\
                                    • read('{}', delimiter=' ')  # space-separated\n\
                                    • read('{}', delimiter='\\t')  # tab-separated\n\
                                    • read('{}', trim_whitespace=true)\n\
                                    • read('{}', skip_rows=N)  # skip header lines",
                                    delimiter, trim_whitespace,
                                    path.display(), path.display(), path.display(), path.display()
                                )
                            ))
                        } else {
                            Err(DtransformError::PolarsError(e))
                        }
                    }
                }
            }
            Some("json") => {
                let file = std::fs::File::open(path)?;
                let df = JsonReader::new(file).finish()?;
                self.check_duplicate_columns(&df)?;
                Ok(df)
            }
            Some("parquet") => {
                let file = std::fs::File::open(path)?;
                let df = ParquetReader::new(file).finish()?;
                self.check_duplicate_columns(&df)?;
                Ok(df)
            }
            Some(_) => {
                // Unknown extension - treat as delimited text file with auto-detection
                let has_header = op.header.unwrap_or(true);
                let skip_rows = op.skip_rows.unwrap_or(0);

                // Determine delimiter and trim_whitespace
                let (delimiter, trim_whitespace) = if op.delimiter.is_none() || op.trim_whitespace.is_none() {
                    // Need to auto-detect delimiter and/or trim_whitespace
                    let content = std::fs::read_to_string(path)?;
                    let (detected_delim, detected_trim) = auto_detect_delimiter(&content, format)?;

                    (
                        op.delimiter.unwrap_or(detected_delim),
                        op.trim_whitespace.unwrap_or(detected_trim)
                    )
                } else {
                    (op.delimiter.unwrap(), op.trim_whitespace.unwrap())
                };

                let result = if trim_whitespace {
                    // Read file, trim each line, and collapse multiple spaces
                    let content = std::fs::read_to_string(path)?;
                    let trimmed_content: String = content
                        .lines()
                        .map(|line| {
                            // Trim leading/trailing whitespace
                            let trimmed = line.trim();
                            // Collapse multiple whitespace into single space
                            trimmed.split_whitespace().collect::<Vec<_>>().join(" ")
                        })
                        .collect::<Vec<_>>()
                        .join("\n");

                    let cursor = std::io::Cursor::new(trimmed_content.as_bytes());
                    CsvReadOptions::default()
                        .with_has_header(has_header)
                        .with_skip_rows(skip_rows)
                        .with_parse_options(
                            CsvParseOptions::default()
                                .with_separator(delimiter as u8)
                        )
                        .into_reader_with_file_handle(cursor)
                        .finish()
                } else {
                    // Standard file path reading
                    CsvReadOptions::default()
                        .with_has_header(has_header)
                        .with_skip_rows(skip_rows)
                        .with_parse_options(
                            CsvParseOptions::default()
                                .with_separator(delimiter as u8)
                        )
                        .try_into_reader_with_file_path(Some(path.into()))?
                        .finish()
                };

                match result {
                    Ok(df) => {
                        self.check_duplicate_columns(&df)?;
                        Ok(df)
                    },
                    Err(e) => {
                        let error_msg = e.to_string();
                        if error_msg.contains("found more fields") || error_msg.contains("Schema") {
                            Err(DtransformError::InvalidOperation(
                                format!(
                                    "CSV parsing error: Rows have different numbers of fields.\n\n\
                                    The auto-detected settings may be incorrect:\n\
                                    • Detected delimiter: {:?}\n\
                                    • Detected trim_whitespace: {}\n\n\
                                    Try specifying explicitly:\n\
                                    • read('{}', delimiter=' ')  # space-separated\n\
                                    • read('{}', delimiter='\\t')  # tab-separated\n\
                                    • read('{}', trim_whitespace=true)\n\
                                    • read('{}', skip_rows=N)  # skip header lines",
                                    delimiter, trim_whitespace,
                                    path.display(), path.display(), path.display(), path.display()
                                )
                            ))
                        } else {
                            Err(DtransformError::PolarsError(e))
                        }
                    }
                }
            }
        }
    }

    fn execute_write(&self, df: DataFrame, op: WriteOp) -> Result<DataFrame> {
        let path = std::path::Path::new(&op.path);
        let format = op.format.as_deref().or_else(|| path.extension()?.to_str());

        match format {
            Some("csv") | Some("tsv") | None => {
                let mut file = std::fs::File::create(path)?;
                let delimiter = op.delimiter.unwrap_or(if format == Some("tsv") { '\t' } else { ',' });
                let has_header = op.header.unwrap_or(true);  // Default to true if not specified

                CsvWriter::new(&mut file)
                    .with_separator(delimiter as u8)
                    .include_header(has_header)
                    .finish(&mut df.clone())?;
            }
            Some("json") => {
                let mut file = std::fs::File::create(path)?;
                JsonWriter::new(&mut file)
                    .finish(&mut df.clone())?;
            }
            Some("parquet") => {
                let mut file = std::fs::File::create(path)?;
                ParquetWriter::new(&mut file)
                    .finish(&mut df.clone())?;
            }
            Some(_) => {
                // Unknown extension - treat as delimited text file
                let mut file = std::fs::File::create(path)?;
                let delimiter = op.delimiter.unwrap_or(',');
                let has_header = op.header.unwrap_or(true);

                CsvWriter::new(&mut file)
                    .with_separator(delimiter as u8)
                    .include_header(has_header)
                    .finish(&mut df.clone())?;
            }
        }

        Ok(df)
    }

    fn execute_select(&self, df: DataFrame, op: SelectOp) -> Result<DataFrame> {
        let schema = df.schema();
        let mut selected_columns = Vec::new();
        let mut aliases = Vec::new();

        for (selector, alias) in op.selectors {
            let cols = self.resolve_selector(&selector, &schema, &df)?;

            // If there's an alias, it applies to all columns from this selector
            // (most commonly just one column, but could be multiple with regex, etc.)
            for col in cols {
                selected_columns.push(col);
                aliases.push(alias.clone());
            }
        }

        if selected_columns.is_empty() {
            return Err(DtransformError::InvalidOperation(
                "No columns selected".to_string(),
            ));
        }

        let mut result = df.select(&selected_columns)?;

        // Apply aliases where provided
        for (i, alias_opt) in aliases.iter().enumerate() {
            if let Some(alias) = alias_opt {
                let old_name = result.get_column_names()[i].to_string();
                result.rename(&old_name, PlSmallStr::from(alias.as_str()))?;
            }
        }

        Ok(result)
    }

    fn resolve_selector(
        &self,
        selector: &ColumnSelector,
        schema: &Schema,
        df: &DataFrame,
    ) -> Result<Vec<String>> {
        match selector {
            ColumnSelector::Name(name) => {
                if schema.contains(name) {
                    Ok(vec![name.clone()])
                } else {
                    Err(DtransformError::ColumnNotFound(name.clone()))
                }
            }

            ColumnSelector::Index(idx) => {
                let name = schema
                    .get_at_index(*idx)
                    .ok_or_else(|| {
                        DtransformError::InvalidOperation(format!("Column index {} out of bounds", idx))
                    })?
                    .0
                    .clone();
                Ok(vec![name.as_str().to_string()])
            }

            ColumnSelector::Range(start, end) => {
                let names: Vec<String> = schema
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| i >= start && i <= end)
                    .map(|(_, (name, _))| name.as_str().to_string())
                    .collect();

                if names.is_empty() {
                    return Err(DtransformError::InvalidOperation(
                        format!("Range ${}..${} is out of bounds or invalid", start + 1, end + 1)
                    ));
                }

                Ok(names)
            }

            ColumnSelector::Regex(pattern) => {
                let re = Regex::new(pattern)?;
                let names: Vec<String> = schema
                    .iter()
                    .filter(|(name, _)| re.is_match(name.as_str()))
                    .map(|(name, _)| name.as_str().to_string())
                    .collect();
                Ok(names)
            }

            ColumnSelector::Type(dtypes) => {
                let names: Vec<String> = schema
                    .iter()
                    .filter(|(_, field)| {
                        dtypes.iter().any(|dt| self.matches_dtype(dt, field))
                    })
                    .map(|(name, _)| name.as_str().to_string())
                    .collect();
                Ok(names)
            }

            ColumnSelector::All => Ok(schema.iter().map(|(name, _)| name.as_str().to_string()).collect()),

            ColumnSelector::Except(inner) => {
                let all_cols: Vec<String> = schema.iter().map(|(name, _)| name.as_str().to_string()).collect();
                let excluded = self.resolve_selector(inner, schema, df)?;
                Ok(all_cols
                    .into_iter()
                    .filter(|col| !excluded.contains(col))
                    .collect())
            }

            ColumnSelector::And(left, right) => {
                let left_cols = self.resolve_selector(left, schema, df)?;
                let right_cols = self.resolve_selector(right, schema, df)?;
                Ok(left_cols
                    .into_iter()
                    .filter(|col| right_cols.contains(col))
                    .collect())
            }
        }
    }

    fn matches_dtype(&self, dt: &crate::parser::ast::DataType, polars_dt: &polars::datatypes::DataType) -> bool {
        use polars::datatypes::DataType as PDT;
        use crate::parser::ast::DataType as AstDT;
        match dt {
            AstDT::Number => matches!(
                polars_dt,
                PDT::Int8
                    | PDT::Int16
                    | PDT::Int32
                    | PDT::Int64
                    | PDT::UInt8
                    | PDT::UInt16
                    | PDT::UInt32
                    | PDT::UInt64
                    | PDT::Float32
                    | PDT::Float64
            ),
            AstDT::String => matches!(polars_dt, PDT::String),
            AstDT::Boolean => matches!(polars_dt, PDT::Boolean),
            AstDT::Date => matches!(polars_dt, PDT::Date),
            AstDT::DateTime => matches!(polars_dt, PDT::Datetime(_, _)),
        }
    }

    fn execute_filter(&self, df: DataFrame, op: FilterOp) -> Result<DataFrame> {
        let mask = self.evaluate_expression(&op.condition, &df)?;
        let mask_bool = mask.bool()?;
        Ok(df.filter(mask_bool)?)
    }

    fn execute_mutate(&self, mut df: DataFrame, op: MutateOp) -> Result<DataFrame> {
        for assignment in op.assignments {
            let series = self.evaluate_expression(&assignment.expression, &df)?;

            // Resolve column name from AssignmentTarget
            let col_name = match &assignment.column {
                AssignmentTarget::Name(name) => name.clone(),
                AssignmentTarget::Position(pos) => {
                    let col_names = df.get_column_names();
                    if *pos == 0 || *pos > col_names.len() {
                        return Err(DtransformError::InvalidOperation(format!(
                            "DataFrame has {} columns, but ${} was specified",
                            col_names.len(), pos
                        )));
                    }
                    col_names[pos - 1].to_string()
                }
            };

            let renamed_series = series.with_name(PlSmallStr::from(col_name.as_str()));
            let _ = df.with_column(renamed_series)?;
        }

        Ok(df)
    }

    fn execute_rename(&self, df: DataFrame, op: RenameOp) -> Result<DataFrame> {
        let mut result = df;
        for (col_ref, new_name) in op.mappings {
            let old_name = self.resolve_column_name(&col_ref, &result)?;
            result.rename(&old_name, PlSmallStr::from(new_name.as_str()))?;
        }
        Ok(result)
    }

    fn execute_rename_all(&self, mut df: DataFrame, op: RenameAllOp) -> Result<DataFrame> {
        match &op.strategy {
            RenameStrategy::Replace { old, new } => {
                let old_names: Vec<String> = df
                    .get_column_names()
                    .iter()
                    .map(|s| s.as_str().to_string())
                    .collect();

                for old_name in old_names {
                    let new_name = old_name.replace(old, new);
                    df.rename(&old_name, PlSmallStr::from(new_name.as_str()))?;
                }

                Ok(df)
            }
            RenameStrategy::Sequential { prefix, start, end } => {
                let num_cols = df.width();
                let range_size = end - start + 1;

                if range_size != num_cols {
                    return Err(DtransformError::InvalidOperation(format!(
                        "Range {}..{} ({} columns) doesn't match table width ({} columns). Use select() first.",
                        start, end, range_size, num_cols
                    )));
                }

                let old_names: Vec<String> = df
                    .get_column_names()
                    .iter()
                    .map(|s| s.as_str().to_string())
                    .collect();

                for (i, old_name) in old_names.iter().enumerate() {
                    let new_name = format!("{}{}", prefix, start + i);
                    df.rename(old_name, PlSmallStr::from(new_name.as_str()))?;
                }

                Ok(df)
            }
        }
    }

    fn execute_sort(&self, df: DataFrame, op: SortOp) -> Result<DataFrame> {
        let col_names: Vec<String> = op
            .columns
            .iter()
            .map(|(col_ref, _)| self.resolve_column_name(col_ref, &df))
            .collect::<Result<Vec<_>>>()?;

        let descending: Vec<bool> = op
            .columns
            .iter()
            .map(|(_, desc)| *desc)
            .collect();

        Ok(df.sort(col_names, SortMultipleOptions::default().with_order_descending_multi(descending))?)
    }

    fn execute_take(&self, df: DataFrame, op: TakeOp) -> Result<DataFrame> {
        Ok(df.head(Some(op.n)))
    }

    fn execute_skip(&self, df: DataFrame, op: SkipOp) -> Result<DataFrame> {
        let height = df.height();
        if op.n >= height {
            Ok(df.head(Some(0)))
        } else {
            Ok(df.slice(op.n as i64, height - op.n))
        }
    }

    fn execute_slice(&self, df: DataFrame, op: SliceOp) -> Result<DataFrame> {
        let start = op.start.min(df.height());
        let len = (op.end.saturating_sub(start)).min(df.height() - start);
        Ok(df.slice(start as i64, len))
    }

    fn execute_drop(&self, df: DataFrame, op: DropOp) -> Result<DataFrame> {
        let schema = df.schema();
        let mut columns_to_drop: Vec<String> = Vec::new();

        // Resolve all selectors to column names
        for selector in op.columns {
            let names = self.resolve_selector(&selector, &schema, &df)?;
            columns_to_drop.extend(names);
        }

        // Drop all columns at once
        let mut result = df;
        for col_name in columns_to_drop {
            result = result.drop(&col_name)?;
        }
        Ok(result)
    }

    fn execute_distinct(&self, df: DataFrame, op: DistinctOp) -> Result<DataFrame> {
        use polars::prelude::UniqueKeepStrategy;

        match op.columns {
            // No columns specified - deduplicate on all columns
            None => {
                df.unique::<Vec<String>, String>(None, UniqueKeepStrategy::First, None)
                    .map_err(DtransformError::from)
            }

            // Specific columns - deduplicate based on those columns
            Some(ref selectors) => {
                // Resolve selectors to column names
                let schema = df.schema();
                let mut column_names: Vec<String> = Vec::new();

                for selector in selectors {
                    let names = self.resolve_selector(selector, &schema, &df)?;
                    column_names.extend(names);
                }

                // Use Polars unique with subset
                df.unique::<Vec<String>, String>(
                    Some(&column_names),
                    UniqueKeepStrategy::First,
                    None
                ).map_err(DtransformError::from)
            }
        }
    }

    fn resolve_column_name(&self, col_ref: &ColumnRef, df: &DataFrame) -> Result<String> {
        match col_ref {
            ColumnRef::Name(name) => Ok(name.clone()),
            ColumnRef::Index(idx) => {
                let col_names = df.get_column_names();
                if *idx < col_names.len() {
                    Ok(col_names[*idx].to_string())
                } else {
                    Err(DtransformError::InvalidOperation(format!(
                        "Column index {} out of bounds (table has {} columns)",
                        idx, col_names.len()
                    )))
                }
            }
            ColumnRef::Position(pos) => {
                // $1 = first column (index 0), $2 = second column (index 1), etc.
                if *pos == 0 {
                    return Err(DtransformError::InvalidOperation(
                        "Positional columns start at $1, not $0".to_string()
                    ));
                }
                let zero_based_idx = pos - 1;
                let col_names = df.get_column_names();
                if zero_based_idx < col_names.len() {
                    Ok(col_names[zero_based_idx].to_string())
                } else {
                    Err(DtransformError::InvalidOperation(format!(
                        "Column ${} out of bounds (table has {} columns)",
                        pos, col_names.len()
                    )))
                }
            }
        }
    }

    fn evaluate_expression(&self, expr: &Expression, df: &DataFrame) -> Result<Series> {
        match expr {
            Expression::Literal(lit) => self.literal_to_series(lit, df.height()),

            Expression::List(literals) => {
                use crate::parser::ast::Literal as AstLiteral;
                // Convert list of literals to a Series for 'in' operator
                // This is a temporary series, not a full DataFrame series
                if literals.is_empty() {
                    return Ok(Series::new_empty(PlSmallStr::from("list"), &polars::datatypes::DataType::Null));
                }
                // Convert literals to Series based on their type
                match &literals[0] {
                    AstLiteral::Number(_) => {
                        let values: Vec<f64> = literals.iter().map(|lit| {
                            match lit {
                                AstLiteral::Number(n) => *n,
                                _ => 0.0, // Type mismatch, but handle gracefully
                            }
                        }).collect();
                        Ok(Series::new(PlSmallStr::from("list"), values))
                    }
                    AstLiteral::String(_) => {
                        let values: Vec<String> = literals.iter().map(|lit| {
                            match lit {
                                AstLiteral::String(s) => s.clone(),
                                _ => String::new(),
                            }
                        }).collect();
                        Ok(Series::new(PlSmallStr::from("list"), values))
                    }
                    AstLiteral::Boolean(_) => {
                        let values: Vec<bool> = literals.iter().map(|lit| {
                            match lit {
                                AstLiteral::Boolean(b) => *b,
                                _ => false,
                            }
                        }).collect();
                        Ok(Series::new(PlSmallStr::from("list"), values))
                    }
                    AstLiteral::Null => {
                        Ok(Series::new_null(PlSmallStr::from("list"), literals.len()))
                    }
                }
            }

            Expression::Column(col_ref) => {
                // Check if this is actually a variable reference
                if let ColumnRef::Name(name) = col_ref {
                    if let Some(var_df) = self.variables.get(name) {
                        // It's a stored variable - extract first column
                        let col = var_df.get_columns().first()
                            .ok_or_else(|| DtransformError::InvalidOperation(
                                format!("Variable '{}' has no columns", name)
                            ))?;
                        return Ok(col.as_materialized_series().clone());
                    }
                }

                // Not a variable - treat as column reference
                let col_name = self.resolve_column_name(col_ref, df)?;
                df.column(&col_name)
                    .map(|col| col.as_materialized_series().clone())
                    .map_err(|e| DtransformError::PolarsError(e))
            }

            Expression::Variable(var_name) => {
                // Resolve variable and extract first column
                let var_df = self.variables.get(var_name)
                    .ok_or_else(|| DtransformError::VariableNotFound(var_name.clone()))?;
                let col = var_df.get_columns().first()
                    .ok_or_else(|| DtransformError::InvalidOperation(
                        format!("Variable '{}' has no columns", var_name)
                    ))?;
                Ok(col.as_materialized_series().clone())
            }

            Expression::BinaryOp { left, op, right } => {
                let left_series = self.evaluate_expression(left, df)?;
                let right_series = self.evaluate_expression(right, df)?;
                self.apply_binary_op(&left_series, op, &right_series, df)
            }

            Expression::MethodCall { object, method, args } => {
                let obj_series = self.evaluate_expression(object, df)?;
                self.apply_method(&obj_series, method, args, df)
            }

            Expression::Split { string, delimiter, index } => {
                // Evaluate string and delimiter expressions
                let string_series = self.evaluate_expression(string, df)?;
                let delimiter_series = self.evaluate_expression(delimiter, df)?;

                // Get delimiter as string (should be a single value)
                let delim = match delimiter_series.dtype() {
                    polars::datatypes::DataType::String => {
                        delimiter_series.str()
                            .map_err(|_| DtransformError::InvalidOperation("Delimiter must be a string".to_string()))?
                            .get(0)
                            .ok_or_else(|| DtransformError::InvalidOperation("Delimiter is null".to_string()))?
                            .to_string()
                    }
                    _ => return Err(DtransformError::InvalidOperation("Delimiter must be a string".to_string())),
                };

                // Apply split to string series
                let string_ca = string_series.str()
                    .map_err(|_| DtransformError::InvalidOperation("Split can only be applied to string columns".to_string()))?;

                // Split each string and extract the specified index
                let result: Vec<Option<String>> = string_ca.into_iter().map(|opt_str| {
                    opt_str.and_then(|s| {
                        let parts: Vec<&str> = s.split(&delim).collect();
                        // Return None if index is out of bounds, otherwise return the element
                        parts.get(*index).map(|&part| part.to_string())
                    })
                }).collect();

                Ok(Series::new(PlSmallStr::from("split"), result))
            }

            Expression::Lookup { table, key, on, return_field } => {
                use crate::parser::ast::LookupField;

                // Get the lookup table from variables
                let lookup_df = self.variables.get(table)
                    .ok_or_else(|| DtransformError::VariableNotFound(table.clone()))?;

                // Resolve the 'on' field name
                let on_col_name = match on {
                    LookupField::Name(name) => name.clone(),
                    LookupField::Position(pos) => {
                        let schema = lookup_df.schema();
                        let col_names: Vec<_> = schema.iter_names().collect();
                        if *pos == 0 || *pos > col_names.len() {
                            return Err(DtransformError::InvalidOperation(format!(
                                "Lookup table '{}' has {} columns, but on=${} was specified",
                                table, col_names.len(), pos
                            )));
                        }
                        col_names[pos - 1].to_string()
                    }
                };

                // Resolve the 'return' field name
                let return_col_name = match return_field {
                    LookupField::Name(name) => name.clone(),
                    LookupField::Position(pos) => {
                        let schema = lookup_df.schema();
                        let col_names: Vec<_> = schema.iter_names().collect();
                        if *pos == 0 || *pos > col_names.len() {
                            return Err(DtransformError::InvalidOperation(format!(
                                "Lookup table '{}' has {} columns, but return=${} was specified",
                                table, col_names.len(), pos
                            )));
                        }
                        col_names[pos - 1].to_string()
                    }
                };

                // Verify the lookup table has both columns
                if !lookup_df.schema().contains(&on_col_name) {
                    return Err(DtransformError::ColumnNotFound(format!(
                        "Lookup table '{}' does not have column '{}' (specified in on=)",
                        table, on_col_name
                    )));
                }
                if !lookup_df.schema().contains(&return_col_name) {
                    return Err(DtransformError::ColumnNotFound(format!(
                        "Lookup table '{}' does not have column '{}' (specified in return=)",
                        table, return_col_name
                    )));
                }

                // Get the 'on' column from the lookup table (this is the key column)
                let lookup_key_col = lookup_df.column(&on_col_name)
                    .map_err(|e| DtransformError::PolarsError(e))?
                    .as_materialized_series();

                // Get the return field column from the lookup table
                let lookup_value_col = lookup_df.column(&return_col_name)
                    .map_err(|e| DtransformError::PolarsError(e))?
                    .as_materialized_series();

                // Evaluate the key expression for each row
                let key_series = self.evaluate_expression(key, df)?;

                // Build a lookup map based on the data type
                use std::collections::HashMap;
                use polars::datatypes::DataType;

                match (lookup_key_col.dtype(), lookup_value_col.dtype()) {
                    (DataType::String, DataType::String) => {
                        let lookup_keys = lookup_key_col.str()
                            .map_err(|_| DtransformError::TypeMismatch {
                                expected: "String".to_string(),
                                got: format!("{:?}", lookup_key_col.dtype()),
                            })?;
                        let lookup_values = lookup_value_col.str()
                            .map_err(|_| DtransformError::TypeMismatch {
                                expected: "String".to_string(),
                                got: format!("{:?}", lookup_value_col.dtype()),
                            })?;

                        // Build lookup map
                        let mut map: HashMap<String, String> = HashMap::new();
                        for i in 0..lookup_df.height() {
                            if let (Some(k), Some(v)) = (lookup_keys.get(i), lookup_values.get(i)) {
                                map.insert(k.to_string(), v.to_string());
                            }
                        }

                        // Apply lookup
                        let input_keys = key_series.str()
                            .map_err(|_| DtransformError::TypeMismatch {
                                expected: "String".to_string(),
                                got: format!("{:?}", key_series.dtype()),
                            })?;

                        let result: Vec<Option<String>> = input_keys.into_iter()
                            .map(|opt_key| {
                                opt_key.and_then(|k| map.get(k).cloned())
                            })
                            .collect();

                        Ok(Series::new(PlSmallStr::from(return_col_name.as_str()), result))
                    }
                    (DataType::String, value_dtype) if matches!(
                        value_dtype,
                        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 |
                        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 |
                        DataType::Float32 | DataType::Float64
                    ) => {
                        let lookup_keys = lookup_key_col.str()
                            .map_err(|_| DtransformError::TypeMismatch {
                                expected: "String".to_string(),
                                got: format!("{:?}", lookup_key_col.dtype()),
                            })?;

                        // Convert value column to f64
                        let lookup_values_f64 = lookup_value_col.cast(&DataType::Float64)
                            .map_err(|e| DtransformError::PolarsError(e))?;
                        let lookup_values = lookup_values_f64.f64()
                            .map_err(|_| DtransformError::InvalidOperation("Failed to cast to Float64".to_string()))?;

                        // Build lookup map
                        let mut map: HashMap<String, f64> = HashMap::new();
                        for i in 0..lookup_df.height() {
                            if let (Some(k), Some(v)) = (lookup_keys.get(i), lookup_values.get(i)) {
                                map.insert(k.to_string(), v);
                            }
                        }

                        // Apply lookup
                        let input_keys = key_series.str()
                            .map_err(|_| DtransformError::TypeMismatch {
                                expected: "String".to_string(),
                                got: format!("{:?}", key_series.dtype()),
                            })?;

                        let result: Vec<Option<f64>> = input_keys.into_iter()
                            .map(|opt_key| {
                                opt_key.and_then(|k| map.get(k).copied())
                            })
                            .collect();

                        Ok(Series::new(PlSmallStr::from(return_col_name.as_str()), result))
                    }
                    _ => {
                        // Generic fallback for other type combinations
                        // This is less efficient but more general
                        Err(DtransformError::InvalidOperation(
                            format!(
                                "Unsupported lookup type combination: key={:?}, value={:?}",
                                lookup_key_col.dtype(),
                                lookup_value_col.dtype()
                            )
                        ))
                    }
                }
            }

            Expression::Replace { text, old, new } => {
                // Evaluate text and new expressions
                let text_series = self.evaluate_expression(text, df)?;
                let new_series = self.evaluate_expression(new, df)?;

                // Ensure text is a string column
                let text_ca = text_series.str()
                    .map_err(|_| DtransformError::InvalidOperation(
                        "replace() can only be applied to string columns".to_string()
                    ))?;

                // Extract new string
                use polars::datatypes::DataType;
                let new_str = match new_series.dtype() {
                    DataType::String => {
                        new_series.str()
                            .map_err(|_| DtransformError::InvalidOperation("Replacement text must be a string".to_string()))?
                            .get(0)
                            .ok_or_else(|| DtransformError::InvalidOperation("Replacement text is null".to_string()))?
                            .to_string()
                    }
                    _ => return Err(DtransformError::InvalidOperation("Replacement text must be a string".to_string())),
                };

                // Check if old pattern is a regex or a literal string
                match old.as_ref() {
                    Expression::Regex(pattern) => {
                        // Use regex replacement
                        let re = Regex::new(pattern)
                            .map_err(|e| DtransformError::InvalidOperation(
                                format!("Invalid regex pattern '{}': {}", pattern, e)
                            ))?;

                        let result: Vec<Option<String>> = text_ca.into_iter().map(|opt_str| {
                            opt_str.map(|s| re.replace_all(s, &new_str).to_string())
                        }).collect();

                        Ok(Series::new(PlSmallStr::from("replace"), result))
                    }
                    _ => {
                        // Evaluate as expression and use literal string replacement
                        let old_series = self.evaluate_expression(old, df)?;
                        let old_str = match old_series.dtype() {
                            DataType::String => {
                                old_series.str()
                                    .map_err(|_| DtransformError::InvalidOperation("Pattern must be a string".to_string()))?
                                    .get(0)
                                    .ok_or_else(|| DtransformError::InvalidOperation("Pattern is null".to_string()))?
                                    .to_string()
                            }
                            _ => return Err(DtransformError::InvalidOperation("Pattern must be a string".to_string())),
                        };

                        let result: Vec<Option<String>> = text_ca.into_iter().map(|opt_str| {
                            opt_str.map(|s| s.replace(&old_str, &new_str))
                        }).collect();

                        Ok(Series::new(PlSmallStr::from("replace"), result))
                    }
                }
            }

            Expression::Regex(pattern) => {
                // Regex literal shouldn't be evaluated directly as a series
                Err(DtransformError::InvalidOperation(
                    format!("Regex pattern '{}' cannot be used directly. Use it with replace() function.", pattern)
                ))
            }
        }
    }

    fn literal_to_series(&self, lit: &crate::parser::ast::Literal, len: usize) -> Result<Series> {
        use crate::parser::ast::Literal as Lit;
        match lit {
            Lit::Number(n) => Ok(Series::new(PlSmallStr::from("literal"), vec![*n; len])),
            Lit::String(s) => Ok(Series::new(PlSmallStr::from("literal"), vec![s.as_str(); len])),
            Lit::Boolean(b) => Ok(Series::new(PlSmallStr::from("literal"), vec![*b; len])),
            Lit::Null => Ok(Series::new_null(PlSmallStr::from("literal"), len)),
        }
    }

    fn apply_binary_op(&self, left: &Series, op: &BinOp, right: &Series, _df: &DataFrame) -> Result<Series> {
        use polars::datatypes::DataType;

        let result = match op {
            BinOp::Add => {
                // Handle string concatenation
                match (left.dtype(), right.dtype()) {
                    (DataType::String, DataType::String) => {
                        let left_str = left.str().map_err(|_| DtransformError::TypeMismatch {
                            expected: "String".to_string(),
                            got: format!("{:?}", left.dtype()),
                        })?;
                        let right_str = right.str().map_err(|_| DtransformError::TypeMismatch {
                            expected: "String".to_string(),
                            got: format!("{:?}", right.dtype()),
                        })?;

                        // Concatenate strings element-wise
                        let result: Vec<Option<String>> = left_str.into_iter()
                            .zip(right_str.into_iter())
                            .map(|(l, r)| {
                                match (l, r) {
                                    (Some(ls), Some(rs)) => Some(format!("{}{}", ls, rs)),
                                    _ => None,
                                }
                            })
                            .collect();

                        Series::new(PlSmallStr::from("concat"), result)
                    }
                    // Numeric addition (default behavior)
                    _ => (left + right)?,
                }
            }
            BinOp::Sub => (left - right)?,
            BinOp::Mul => (left * right)?,
            BinOp::Div => (left / right)?,
            BinOp::Gt => left.gt(right)?.into_series(),
            BinOp::Lt => left.lt(right)?.into_series(),
            BinOp::Gte => left.gt_eq(right)?.into_series(),
            BinOp::Lte => left.lt_eq(right)?.into_series(),
            BinOp::Eq => left.equal(right)?.into_series(),
            BinOp::Neq => left.not_equal(right)?.into_series(),
            BinOp::And => {
                let left_bool = left.bool()?;
                let right_bool = right.bool()?;
                (left_bool & right_bool).into_series()
            }
            BinOp::Or => {
                let left_bool = left.bool()?;
                let right_bool = right.bool()?;
                (left_bool | right_bool).into_series()
            }
            BinOp::In => {
                // Check if left values are in right collection
                // right is a series of values to check against
                // left is the column to test

                use std::collections::HashSet;
                use polars::datatypes::DataType;

                match left.dtype() {
                    DataType::String => {
                        let left_str = left.str()?;
                        let right_str = right.str()?;

                        // Collect right values into a HashSet for O(1) lookup
                        let right_set: HashSet<Option<&str>> = right_str.into_iter().collect();

                        // Check each left value
                        let mask: BooleanChunked = left_str
                            .into_iter()
                            .map(|val| right_set.contains(&val))
                            .collect();

                        mask.into_series()
                    }
                    DataType::Int64 | DataType::Int32 | DataType::Float64 | DataType::Float32 => {
                        // Convert both to f64 for comparison
                        let left_f64 = left.cast(&DataType::Float64)?;
                        let right_f64 = right.cast(&DataType::Float64)?;

                        let left_num = left_f64.f64()?;
                        let right_num = right_f64.f64()?;

                        // Collect right values into a Vec for comparison
                        let right_values: Vec<Option<f64>> = right_num.into_iter().collect();

                        // Check each left value
                        let mask: BooleanChunked = left_num
                            .into_iter()
                            .map(|left_val| {
                                right_values.iter().any(|right_val| {
                                    match (left_val, right_val) {
                                        (Some(l), Some(r)) => (l - r).abs() < f64::EPSILON,
                                        (None, None) => true,
                                        _ => false,
                                    }
                                })
                            })
                            .collect();

                        mask.into_series()
                    }
                    _ => {
                        return Err(DtransformError::TypeMismatch {
                            expected: "String or Number".to_string(),
                            got: format!("{:?}", left.dtype()),
                        });
                    }
                }
            }
        };
        Ok(result)
    }

    fn apply_method(&self, _obj: &Series, method: &str, _args: &[Expression], _df: &DataFrame) -> Result<Series> {
        // String methods have been removed. Use function-based operations instead:
        // - For replace: use replace(column, 'old', 'new')
        Err(DtransformError::InvalidOperation(format!(
            "Method '{}' is not supported. Use function-based operations instead.\n\
            Example: mutate(clean = replace(text, 'old', 'new'))",
            method
        )))
    }

    pub fn get_variable(&self, name: &str) -> Option<&DataFrame> {
        self.variables.get(name)
    }

    pub fn set_variable(&mut self, name: String, df: DataFrame) {
        self.variables.insert(name, df);
    }

    pub fn remove_variable(&mut self, name: &str) {
        self.variables.remove(name);
    }

    pub fn list_variables(&self) -> Vec<String> {
        self.variables.keys().cloned().collect()
    }

    pub fn get_all_variables(&self) -> HashMap<String, DataFrame> {
        self.variables.clone()
    }

    pub fn restore_variables(&mut self, snapshot: HashMap<String, DataFrame>) {
        self.variables = snapshot;
    }
}
