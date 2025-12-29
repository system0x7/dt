pub mod ast;

use pest::Parser;
use pest_derive::Parser;

use ast::*;
use crate::error::{DtransformError, Result};

#[derive(Parser)]
#[grammar = "parser/grammar.pest"]
pub struct DtransformParser;

// Parse a multi-statement program (for files/CLI)
pub fn parse_program(input: &str) -> Result<Program> {
    let pairs = DtransformParser::parse(Rule::program, input)
        .map_err(|e| DtransformError::PestError(e.to_string()))?;

    let program_pair = pairs.into_iter().next().unwrap();
    parse_program_inner(program_pair)
}

// Parse a single statement (for REPL)
pub fn parse(input: &str) -> Result<Statement> {
    let pairs = DtransformParser::parse(Rule::statement, input)
        .map_err(|e| DtransformError::PestError(e.to_string()))?;

    let statement_pair = pairs.into_iter().next().unwrap();
    parse_statement(statement_pair)
}

fn parse_program_inner(pair: pest::iterators::Pair<Rule>) -> Result<Program> {
    let mut statements = Vec::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::statement_inner {
            statements.push(parse_statement_inner(inner)?);
        }
    }

    Ok(Program { statements })
}

fn parse_statement(pair: pest::iterators::Pair<Rule>) -> Result<Statement> {
    let inner = pair.into_inner().next().unwrap();
    parse_statement_inner(inner)
}

fn parse_statement_inner(pair: pest::iterators::Pair<Rule>) -> Result<Statement> {
    let inner = pair.into_inner().next().unwrap();

    match inner.as_rule() {
        Rule::assignment => {
            let mut inner_pairs = inner.into_inner();
            let name = inner_pairs.next().unwrap().as_str().to_string();
            let pipeline = parse_pipeline(inner_pairs.next().unwrap())?;
            Ok(Statement::Assignment { name, pipeline })
        }
        Rule::pipeline => {
            let pipeline = parse_pipeline(inner)?;
            Ok(Statement::Pipeline(pipeline))
        }
        _ => Err(DtransformError::ParseError(format!("Unexpected rule: {:?}", inner.as_rule())))
    }
}

fn parse_pipeline(pair: pest::iterators::Pair<Rule>) -> Result<Pipeline> {
    let mut operations = Vec::new();
    let mut source = None;

    for inner_pair in pair.into_inner() {
        match inner_pair.as_rule() {
            Rule::operation => {
                operations.push(parse_operation(inner_pair)?);
            }
            _ => {}
        }
    }

    // Extract source from first operation if it's a read or variable
    if !operations.is_empty() {
        match &operations[0] {
            Operation::Read(read_op) => {
                source = Some(Source::Read(read_op.clone()));
                operations.remove(0);
            }
            Operation::Variable(var_name) => {
                source = Some(Source::Variable(var_name.clone()));
                operations.remove(0);
            }
            _ => {}
        }
    }

    Ok(Pipeline { source, operations })
}


fn parse_operation(pair: pest::iterators::Pair<Rule>) -> Result<Operation> {
    let inner = pair.into_inner().next().unwrap();

    match inner.as_rule() {
        Rule::read_op => Ok(Operation::Read(parse_read_op(inner)?)),
        Rule::write_op => Ok(Operation::Write(parse_write_op(inner)?)),
        Rule::select_op => Ok(Operation::Select(parse_select_op(inner)?)),
        Rule::filter_op => Ok(Operation::Filter(parse_filter_op(inner)?)),
        Rule::mutate_op => Ok(Operation::Mutate(parse_mutate_op(inner)?)),
        Rule::rename_op => Ok(Operation::Rename(parse_rename_op(inner)?)),
        Rule::rename_all_op => Ok(Operation::RenameAll(parse_rename_all_op(inner)?)),
        Rule::sort_op => Ok(Operation::Sort(parse_sort_op(inner)?)),
        Rule::take_op => Ok(Operation::Take(parse_take_op(inner)?)),
        Rule::skip_op => Ok(Operation::Skip(parse_skip_op(inner)?)),
        Rule::slice_op => Ok(Operation::Slice(parse_slice_op(inner)?)),
        Rule::drop_op => Ok(Operation::Drop(parse_drop_op(inner)?)),
        Rule::distinct_op => Ok(Operation::Distinct(parse_distinct_op(inner)?)),
        Rule::variable_ref => {
            // This is a variable reference used as a source
            Ok(Operation::Variable(inner.as_str().trim().to_string()))
        }
        _ => Err(DtransformError::ParseError(format!("Unknown operation: {:?}", inner.as_rule())))
    }
}

fn parse_read_op(pair: pest::iterators::Pair<Rule>) -> Result<ReadOp> {
    let mut inner_pairs = pair.into_inner();
    let path = parse_string(inner_pairs.next().unwrap())?;

    let mut format = None;
    let mut delimiter = None;
    let mut header = None;
    let mut skip_rows = None;
    let mut trim_whitespace = None;

    if let Some(params_pair) = inner_pairs.next() {
        for param in params_pair.into_inner() {
            let mut param_inner = param.into_inner();
            let name = param_inner.next().unwrap().as_str();
            let value = param_inner.next().unwrap();

            match name {
                "format" => format = Some(parse_param_value(value)?),
                "delimiter" => {
                    let delim_str = parse_param_value(value)?;
                    delimiter = delim_str.chars().next();
                }
                "header" => {
                    let header_str = parse_param_value(value)?;
                    header = Some(header_str == "true");
                }
                "skip_rows" => {
                    let skip_str = parse_param_value(value)?;
                    skip_rows = Some(skip_str.parse::<usize>().map_err(|_| {
                        DtransformError::ParseError(format!("Invalid skip_rows value: {}", skip_str))
                    })?);
                }
                "trim_whitespace" => {
                    let trim_str = parse_param_value(value)?;
                    trim_whitespace = Some(trim_str == "true");
                }
                _ => {}
            }
        }
    }

    Ok(ReadOp { path, format, delimiter, header, skip_rows, trim_whitespace })
}

fn parse_write_op(pair: pest::iterators::Pair<Rule>) -> Result<WriteOp> {
    let mut inner_pairs = pair.into_inner();
    let path = parse_string(inner_pairs.next().unwrap())?;

    let mut format = None;
    let mut header = None;
    let mut delimiter = None;

    if let Some(params_pair) = inner_pairs.next() {
        for param in params_pair.into_inner() {
            let mut param_inner = param.into_inner();
            let name = param_inner.next().unwrap().as_str();
            let value = param_inner.next().unwrap();

            match name {
                "format" => format = Some(parse_param_value(value)?),
                "header" => header = Some(parse_param_value(value)? == "true"),
                "delimiter" => {
                    let delim_str = parse_param_value(value)?;
                    delimiter = delim_str.chars().next();
                }
                _ => {}
            }
        }
    }

    Ok(WriteOp { path, format, header, delimiter })
}

fn parse_select_op(pair: pest::iterators::Pair<Rule>) -> Result<SelectOp> {
    let mut selectors = Vec::new();

    for inner_pair in pair.into_inner() {
        if inner_pair.as_rule() == Rule::selector_list {
            for selector_item_pair in inner_pair.into_inner() {
                let (selector, alias) = parse_selector_item(selector_item_pair)?;
                selectors.push((selector, alias));
            }
        }
    }

    Ok(SelectOp { selectors })
}

fn parse_selector_item(pair: pest::iterators::Pair<Rule>) -> Result<(ColumnSelector, Option<String>)> {
    let inner = pair.into_inner().next().unwrap();

    match inner.as_rule() {
        Rule::aliased_selector => {
            let mut inner_pairs = inner.into_inner();
            let first = inner_pairs.next().unwrap();
            let second = inner_pairs.next().unwrap();

            // Check if first token is a selector or identifier
            // Grammar: selector ~ "as" ~ identifier | identifier ~ "=" ~ selector
            match first.as_rule() {
                Rule::selector => {
                    // New syntax: selector as identifier
                    let selector = parse_selector(first)?;
                    let alias = second.as_str().to_string();
                    Ok((selector, Some(alias)))
                }
                Rule::identifier => {
                    // Old syntax: identifier = selector
                    let alias = first.as_str().to_string();
                    let selector = parse_selector(second)?;
                    Ok((selector, Some(alias)))
                }
                _ => Err(DtransformError::ParseError("Invalid aliased selector".to_string()))
            }
        }
        Rule::selector => {
            let selector = parse_selector(inner)?;
            Ok((selector, None))
        }
        _ => Err(DtransformError::ParseError("Invalid selector item".to_string()))
    }
}

fn parse_selector(pair: pest::iterators::Pair<Rule>) -> Result<ColumnSelector> {
    // If we have a selector wrapper, unwrap it
    let actual_pair = if pair.as_rule() == Rule::selector {
        pair.into_inner().next().unwrap()
    } else {
        pair
    };

    match actual_pair.as_rule() {
        Rule::column_ref => {
            let inner = actual_pair.into_inner().next().unwrap();
            match inner.as_rule() {
                Rule::positional_column => {
                    // $1, $2, etc. - AWK-style (1-based)
                    let text = inner.as_str();
                    let num_str = &text[1..]; // Skip the '$'
                    let position = parse_number_as_usize(num_str)?;
                    if position == 0 {
                        return Err(DtransformError::ParseError(
                            "Positional columns start at $1, not $0".to_string()
                        ));
                    }
                    // For selector, convert to 0-based index
                    Ok(ColumnSelector::Index(position - 1))
                }
                Rule::identifier => Ok(ColumnSelector::Name(inner.as_str().to_string())),
                _ => Err(DtransformError::ParseError("Invalid column reference".to_string()))
            }
        }
        Rule::regex_selector => {
            let pattern = parse_string(actual_pair.into_inner().next().unwrap())?;
            Ok(ColumnSelector::Regex(pattern))
        }
        Rule::positional_range => {
            let mut inner_pairs = actual_pair.into_inner();
            let start_pair = inner_pairs.next().unwrap();
            let end_pair = inner_pairs.next().unwrap();

            // Parse $N format
            let start_str = start_pair.as_str();
            let start_num = parse_number_as_usize(&start_str[1..])?; // Skip '$'
            if start_num == 0 {
                return Err(DtransformError::ParseError(
                    "Positional ranges start at $1, not $0".to_string()
                ));
            }

            let end_str = end_pair.as_str();
            let end_num = parse_number_as_usize(&end_str[1..])?; // Skip '$'
            if end_num == 0 {
                return Err(DtransformError::ParseError(
                    "Positional ranges start at $1, not $0".to_string()
                ));
            }

            // Convert to 0-based indices
            Ok(ColumnSelector::Range(start_num - 1, end_num - 1))
        }
        Rule::type_selector => {
            let mut types = Vec::new();
            for type_pair in actual_pair.into_inner() {
                if type_pair.as_rule() == Rule::type_list {
                    for data_type_pair in type_pair.into_inner() {
                        types.push(parse_data_type(data_type_pair)?);
                    }
                }
            }
            Ok(ColumnSelector::Type(types))
        }
        Rule::except_selector => {
            let inner = actual_pair.into_inner().next().unwrap();
            let selector = parse_selector(inner)?;
            Ok(ColumnSelector::Except(Box::new(selector)))
        }
        _ => Err(DtransformError::ParseError(format!("Unknown selector: {:?}", actual_pair.as_rule())))
    }
}

fn parse_data_type(pair: pest::iterators::Pair<Rule>) -> Result<DataType> {
    match pair.as_str() {
        "Number" => Ok(DataType::Number),
        "String" => Ok(DataType::String),
        "Boolean" => Ok(DataType::Boolean),
        "Date" => Ok(DataType::Date),
        "DateTime" => Ok(DataType::DateTime),
        _ => Err(DtransformError::ParseError("Invalid data type".to_string()))
    }
}

fn parse_filter_op(pair: pest::iterators::Pair<Rule>) -> Result<FilterOp> {
    let condition = parse_expression(pair.into_inner().next().unwrap())?;
    Ok(FilterOp { condition })
}

fn parse_mutate_op(pair: pest::iterators::Pair<Rule>) -> Result<MutateOp> {
    let mut assignments = Vec::new();

    for inner_pair in pair.into_inner() {
        if inner_pair.as_rule() == Rule::assignment_list {
            for assignment_pair in inner_pair.into_inner() {
                assignments.push(parse_assignment(assignment_pair)?);
            }
        }
    }

    Ok(MutateOp { assignments })
}

fn parse_assignment(pair: pest::iterators::Pair<Rule>) -> Result<Assignment> {
    use crate::parser::ast::AssignmentTarget;

    let mut inner_pairs = pair.into_inner();
    let column_pair = inner_pairs.next().unwrap();

    let column = match column_pair.as_rule() {
        Rule::identifier => AssignmentTarget::Name(column_pair.as_str().to_string()),
        Rule::number => AssignmentTarget::Name(format!("col_{}", column_pair.as_str())),
        Rule::positional_column => {
            // $1, $2, etc.
            let text = column_pair.as_str();
            let num_str = &text[1..]; // Skip the '$'
            let pos: usize = num_str.parse()
                .map_err(|_| DtransformError::ParseError(format!("Invalid column number: {}", num_str)))?;

            if pos == 0 {
                return Err(DtransformError::ParseError(
                    "Column positions must be 1-based (e.g., $1, $2, ...)".to_string()
                ));
            }

            AssignmentTarget::Position(pos)
        }
        _ => return Err(DtransformError::ParseError("Invalid column in assignment".to_string()))
    };

    let expression = parse_expression(inner_pairs.next().unwrap())?;

    Ok(Assignment { column, expression })
}

fn parse_rename_op(pair: pest::iterators::Pair<Rule>) -> Result<RenameOp> {
    let mut mappings = Vec::new();

    for inner_pair in pair.into_inner() {
        if inner_pair.as_rule() == Rule::rename_mapping_list {
            for mapping_pair in inner_pair.into_inner() {
                let mut mapping_inner = mapping_pair.into_inner();
                let col_ref = parse_column_ref(mapping_inner.next().unwrap())?;
                let new_name_pair = mapping_inner.next().unwrap();

                let new_name = match new_name_pair.as_rule() {
                    Rule::identifier => new_name_pair.as_str().to_string(),
                    Rule::string => parse_string(new_name_pair)?,
                    _ => return Err(DtransformError::ParseError("Invalid new name in rename".to_string()))
                };

                mappings.push((col_ref, new_name));
            }
        }
    }

    Ok(RenameOp { mappings })
}

fn parse_rename_all_op(pair: pest::iterators::Pair<Rule>) -> Result<RenameAllOp> {
    let strategy_pair = pair.into_inner().next().unwrap();
    let strategy = parse_rename_strategy(strategy_pair)?;
    Ok(RenameAllOp { strategy })
}

fn parse_rename_strategy(pair: pest::iterators::Pair<Rule>) -> Result<RenameStrategy> {
    let inner = pair.into_inner().next().unwrap();

    match inner.as_rule() {
        Rule::replace_strategy => {
            let mut inner_pairs = inner.into_inner();
            let old = parse_string(inner_pairs.next().unwrap())?;
            let new = parse_string(inner_pairs.next().unwrap())?;
            Ok(RenameStrategy::Replace { old, new })
        }
        Rule::sequential_strategy => {
            let mut inner_pairs = inner.into_inner();
            let prefix = parse_string(inner_pairs.next().unwrap())?;
            let start = parse_number_as_usize(inner_pairs.next().unwrap().as_str())?;
            let end = parse_number_as_usize(inner_pairs.next().unwrap().as_str())?;
            Ok(RenameStrategy::Sequential { prefix, start, end })
        }
        _ => Err(DtransformError::ParseError("Unknown rename strategy".to_string()))
    }
}

fn parse_sort_op(pair: pest::iterators::Pair<Rule>) -> Result<SortOp> {
    let mut columns = Vec::new();

    for inner_pair in pair.into_inner() {
        if inner_pair.as_rule() == Rule::sort_column_list {
            for sort_col_pair in inner_pair.into_inner() {
                let mut sort_col_inner = sort_col_pair.into_inner();
                let col_ref = parse_column_ref(sort_col_inner.next().unwrap())?;

                let descending = if let Some(order_pair) = sort_col_inner.next() {
                    order_pair.as_str() == "desc"
                } else {
                    false
                };

                columns.push((col_ref, descending));
            }
        }
    }

    Ok(SortOp { columns })
}

fn parse_take_op(pair: pest::iterators::Pair<Rule>) -> Result<TakeOp> {
    let n = parse_number_as_usize(pair.into_inner().next().unwrap().as_str())?;
    Ok(TakeOp { n })
}

fn parse_skip_op(pair: pest::iterators::Pair<Rule>) -> Result<SkipOp> {
    let n = parse_number_as_usize(pair.into_inner().next().unwrap().as_str())?;
    Ok(SkipOp { n })
}

fn parse_slice_op(pair: pest::iterators::Pair<Rule>) -> Result<SliceOp> {
    let mut inner_pairs = pair.into_inner();
    let start = parse_number_as_usize(inner_pairs.next().unwrap().as_str())?;
    let end = parse_number_as_usize(inner_pairs.next().unwrap().as_str())?;
    Ok(SliceOp { start, end })
}

fn parse_drop_op(pair: pest::iterators::Pair<Rule>) -> Result<DropOp> {
    let mut columns = Vec::new();

    for inner_pair in pair.into_inner() {
        if inner_pair.as_rule() == Rule::selector_list {
            for selector_item_pair in inner_pair.into_inner() {
                let (selector, _alias) = parse_selector_item(selector_item_pair)?;
                columns.push(selector);
            }
        }
    }

    Ok(DropOp { columns })
}

fn parse_distinct_op(pair: pest::iterators::Pair<Rule>) -> Result<DistinctOp> {
    let mut columns = None;

    for inner_pair in pair.into_inner() {
        if inner_pair.as_rule() == Rule::selector_list {
            let mut selectors = Vec::new();
            for selector_item_pair in inner_pair.into_inner() {
                let (selector, _alias) = parse_selector_item(selector_item_pair)?;
                selectors.push(selector);
            }
            columns = Some(selectors);
        }
    }

    Ok(DistinctOp { columns })
}

fn parse_column_ref(pair: pest::iterators::Pair<Rule>) -> Result<ColumnRef> {
    let inner = pair.into_inner().next().unwrap();

    match inner.as_rule() {
        Rule::positional_column => {
            // $1, $2, etc. - AWK-style (1-based)
            let text = inner.as_str();
            let num_str = &text[1..]; // Skip the '$'
            let position = parse_number_as_usize(num_str)?;
            if position == 0 {
                return Err(DtransformError::ParseError(
                    "Positional columns start at $1, not $0".to_string()
                ));
            }
            Ok(ColumnRef::Position(position))
        }
        Rule::identifier => Ok(ColumnRef::Name(inner.as_str().to_string())),
        _ => Err(DtransformError::ParseError("Invalid column reference".to_string()))
    }
}

fn parse_expression(pair: pest::iterators::Pair<Rule>) -> Result<Expression> {
    match pair.as_rule() {
        Rule::expression | Rule::logical_or | Rule::logical_and | Rule::comparison | Rule::term | Rule::factor => {
            let mut pairs = pair.into_inner();
            let first = pairs.next().unwrap();
            let mut left = parse_expression(first)?;

            while let Some(op_pair) = pairs.next() {
                let op = match op_pair.as_rule() {
                    Rule::comparison_op | Rule::add_op | Rule::sub_op | Rule::mul_op | Rule::div_op => {
                        parse_bin_op(op_pair.as_str())?
                    }
                    _ if op_pair.as_str() == "and" || op_pair.as_str() == "or" => {
                        parse_bin_op(op_pair.as_str())?
                    }
                    _ => {
                        // This is the right operand
                        let right = parse_expression(op_pair)?;
                        return Ok(Expression::BinaryOp {
                            left: Box::new(left),
                            op: BinOp::Add, // This shouldn't happen
                            right: Box::new(right),
                        });
                    }
                };

                let right_pair = pairs.next().unwrap();
                let right = parse_expression(right_pair)?;

                left = Expression::BinaryOp {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                };
            }

            Ok(left)
        }
        Rule::primary => {
            let inner = pair.into_inner().next().unwrap();
            parse_expression(inner)
        }
        Rule::invalid_split => {
            return Err(DtransformError::ParseError(
                "split() must be followed by [index]. Example: split(text, ':')[0]".to_string()
            ));
        }
        Rule::split_call => parse_split_call(pair),
        Rule::lookup_call => parse_lookup_call(pair),
        Rule::replace_call => parse_replace_call(pair),
        Rule::regex_literal => {
            let pattern = parse_string(pair.into_inner().next().unwrap())?;
            Ok(Expression::Regex(pattern))
        }
        Rule::method_call => parse_method_call(pair),
        Rule::positional_column => {
            // $1, $2, etc. - AWK-style (1-based)
            let text = pair.as_str();
            let num_str = &text[1..]; // Skip the '$'
            let position = parse_number_as_usize(num_str)?;
            if position == 0 {
                return Err(DtransformError::ParseError(
                    "Positional columns start at $1, not $0".to_string()
                ));
            }
            Ok(Expression::Column(ColumnRef::Position(position)))
        }
        Rule::column_ref => {
            let col_ref = parse_column_ref(pair)?;
            Ok(Expression::Column(col_ref))
        }
        Rule::list_literal => {
            // Parse list literal: ['a', 'b', 'c'] or [1, 2, 3]
            let mut literals = Vec::new();
            for inner in pair.into_inner() {
                if inner.as_rule() == Rule::literal_list {
                    for literal_pair in inner.into_inner() {
                        literals.push(parse_literal(literal_pair)?);
                    }
                }
            }
            Ok(Expression::List(literals))
        }
        Rule::literal => parse_literal_expression(pair),
        Rule::boolean => {
            let val = pair.as_str() == "true";
            Ok(Expression::Literal(Literal::Boolean(val)))
        }
        Rule::null => Ok(Expression::Literal(Literal::Null)),
        Rule::number => {
            let val = parse_number(pair.as_str())?;
            Ok(Expression::Literal(Literal::Number(val)))
        }
        Rule::string => {
            let val = parse_string(pair)?;
            Ok(Expression::Literal(Literal::String(val)))
        }
        Rule::identifier => {
            Ok(Expression::Column(ColumnRef::Name(pair.as_str().to_string())))
        }
        _ => {
            // Try to parse as expression recursively
            let rule = pair.as_rule();
            if let Some(inner) = pair.into_inner().next() {
                parse_expression(inner)
            } else {
                Err(DtransformError::ParseError(format!("Unknown expression type: {:?}", rule)))
            }
        }
    }
}

fn parse_split_call(pair: pest::iterators::Pair<Rule>) -> Result<Expression> {
    let mut inner_pairs = pair.into_inner();

    // Parse string expression
    let string_expr = parse_expression(inner_pairs.next().unwrap())?;

    // Parse delimiter expression
    let delimiter_expr = parse_expression(inner_pairs.next().unwrap())?;

    // Parse index (0-based)
    let index_pair = inner_pairs.next().unwrap();
    let index = parse_number_as_usize(index_pair.as_str())?;

    Ok(Expression::Split {
        string: Box::new(string_expr),
        delimiter: Box::new(delimiter_expr),
        index,
    })
}

fn parse_lookup_call(pair: pest::iterators::Pair<Rule>) -> Result<Expression> {
    let mut inner_pairs = pair.into_inner();

    // Parse table name (identifier)
    let table = inner_pairs.next().unwrap().as_str().to_string();

    // Parse key expression
    let key_expr = parse_expression(inner_pairs.next().unwrap())?;

    // Parse 'on' field (string or column_ref)
    let on = parse_lookup_field(inner_pairs.next().unwrap())?;

    // Parse 'return' field (string or column_ref)
    let return_field = parse_lookup_field(inner_pairs.next().unwrap())?;

    Ok(Expression::Lookup {
        table,
        key: Box::new(key_expr),
        on,
        return_field,
    })
}

fn parse_replace_call(pair: pest::iterators::Pair<Rule>) -> Result<Expression> {
    let mut inner_pairs = pair.into_inner();

    // Parse text expression (the string/column to perform replacement on)
    let text_expr = parse_expression(inner_pairs.next().unwrap())?;

    // Parse old expression (pattern to replace)
    let old_expr = parse_expression(inner_pairs.next().unwrap())?;

    // Parse new expression (replacement text)
    let new_expr = parse_expression(inner_pairs.next().unwrap())?;

    Ok(Expression::Replace {
        text: Box::new(text_expr),
        old: Box::new(old_expr),
        new: Box::new(new_expr),
    })
}

fn parse_lookup_field(pair: pest::iterators::Pair<Rule>) -> Result<crate::parser::ast::LookupField> {
    use crate::parser::ast::LookupField;

    let inner = pair.into_inner().next().unwrap();

    match inner.as_rule() {
        Rule::string => {
            let name = parse_string(inner)?;
            Ok(LookupField::Name(name))
        }
        Rule::column_ref => {
            let col_inner = inner.into_inner().next().unwrap();
            match col_inner.as_rule() {
                Rule::positional_column => {
                    // $1, $2, etc.
                    let text = col_inner.as_str();
                    let num_str = &text[1..]; // Skip the '$'
                    let pos: usize = num_str.parse()
                        .map_err(|_| DtransformError::ParseError(format!("Invalid column number: {}", num_str)))?;

                    if pos == 0 {
                        return Err(DtransformError::ParseError(
                            "Column positions must be 1-based (e.g., $1, $2, ...)".to_string()
                        ));
                    }

                    Ok(LookupField::Position(pos))
                }
                Rule::identifier => {
                    // Named column
                    let name = col_inner.as_str().to_string();
                    Ok(LookupField::Name(name))
                }
                _ => Err(DtransformError::ParseError(format!(
                    "Unexpected column reference type: {:?}",
                    col_inner.as_rule()
                )))
            }
        }
        _ => Err(DtransformError::ParseError(format!(
            "Expected string or column reference, got: {:?}",
            inner.as_rule()
        )))
    }
}

fn parse_method_call(pair: pest::iterators::Pair<Rule>) -> Result<Expression> {
    let mut inner_pairs = pair.into_inner();
    let object_pair = inner_pairs.next().unwrap();

    let mut object = match object_pair.as_rule() {
        Rule::identifier => Expression::Column(ColumnRef::Name(object_pair.as_str().to_string())),
        Rule::column_ref => {
            let col_ref = parse_column_ref(object_pair)?;
            Expression::Column(col_ref)
        }
        _ => parse_expression(object_pair)?
    };

    // Handle chained method calls
    while let Some(method_pair) = inner_pairs.next() {
        if method_pair.as_rule() == Rule::identifier {
            let method = method_pair.as_str().to_string();

            let mut args = Vec::new();
            if let Some(arg_list_pair) = inner_pairs.next() {
                if arg_list_pair.as_rule() == Rule::arg_list {
                    for arg_pair in arg_list_pair.into_inner() {
                        args.push(parse_expression(arg_pair)?);
                    }
                }
            }

            object = Expression::MethodCall {
                object: Box::new(object),
                method,
                args,
            };
        }
    }

    Ok(object)
}

fn parse_literal(pair: pest::iterators::Pair<Rule>) -> Result<Literal> {
    let inner = if pair.as_rule() == Rule::literal {
        pair.into_inner().next().unwrap()
    } else {
        pair
    };

    match inner.as_rule() {
        Rule::boolean => {
            let val = inner.as_str() == "true";
            Ok(Literal::Boolean(val))
        }
        Rule::null => Ok(Literal::Null),
        Rule::number => {
            let val = parse_number(inner.as_str())?;
            Ok(Literal::Number(val))
        }
        Rule::string => {
            let val = parse_string(inner)?;
            Ok(Literal::String(val))
        }
        _ => Err(DtransformError::ParseError("Invalid literal".to_string()))
    }
}

fn parse_literal_expression(pair: pest::iterators::Pair<Rule>) -> Result<Expression> {
    parse_literal(pair).map(Expression::Literal)
}

fn parse_bin_op(op_str: &str) -> Result<BinOp> {
    match op_str {
        "+" => Ok(BinOp::Add),
        "-" => Ok(BinOp::Sub),
        "*" => Ok(BinOp::Mul),
        "/" => Ok(BinOp::Div),
        ">" => Ok(BinOp::Gt),
        "<" => Ok(BinOp::Lt),
        ">=" => Ok(BinOp::Gte),
        "<=" => Ok(BinOp::Lte),
        "==" => Ok(BinOp::Eq),
        "!=" => Ok(BinOp::Neq),
        "and" => Ok(BinOp::And),
        "or" => Ok(BinOp::Or),
        "in" => Ok(BinOp::In),
        _ => Err(DtransformError::ParseError(format!("Unknown operator: {}", op_str)))
    }
}

fn parse_string(pair: pest::iterators::Pair<Rule>) -> Result<String> {
    let inner = pair.into_inner().next().unwrap();
    let s = inner.as_str();

    // Unescape common escape sequences
    let unescaped = s
        .replace("\\n", "\n")
        .replace("\\r", "\r")
        .replace("\\t", "\t")
        .replace("\\\"", "\"")
        .replace("\\'", "'")
        .replace("\\\\", "\\");

    Ok(unescaped)
}

fn parse_param_value(pair: pest::iterators::Pair<Rule>) -> Result<String> {
    match pair.as_rule() {
        Rule::param_value => {
            // param_value wraps the actual value, unwrap it
            let inner = pair.into_inner().next().unwrap();
            parse_param_value(inner)
        }
        Rule::string => parse_string(pair),
        Rule::number => Ok(pair.as_str().to_string()),
        Rule::boolean => Ok(pair.as_str().to_string()),
        Rule::identifier => Ok(pair.as_str().to_string()),
        _ => Err(DtransformError::ParseError(format!("Invalid parameter value: {:?}", pair.as_rule())))
    }
}

fn parse_number(s: &str) -> Result<f64> {
    // Handle suffixes (k, m, b)
    let multiplier = if s.ends_with('k') || s.ends_with('K') {
        1000.0
    } else if s.ends_with('m') || s.ends_with('M') {
        1_000_000.0
    } else if s.ends_with('b') || s.ends_with('B') {
        1_000_000_000.0
    } else {
        1.0
    };

    let num_str = if multiplier != 1.0 {
        &s[..s.len() - 1]
    } else {
        s
    };

    num_str.parse::<f64>()
        .map(|n| n * multiplier)
        .map_err(|_| DtransformError::ParseError(format!("Invalid number: {}", s)))
}

fn parse_number_as_usize(s: &str) -> Result<usize> {
    parse_number(s).and_then(|n| {
        if n < 0.0 || n.fract() != 0.0 {
            Err(DtransformError::ParseError(format!("Expected positive integer, got: {}", s)))
        } else {
            Ok(n as usize)
        }
    })
}
