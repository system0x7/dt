use colored::*;
use polars::prelude::*;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use std::collections::HashMap;

use crate::error::Result;
use crate::executor::Executor;
use crate::parser::{parse, ast::Statement};

pub struct Repl {
    editor: DefaultEditor,
    executor: Executor,

    // Current state
    current: Option<DataFrame>,

    // History for undo/redo
    history: Vec<DataFrame>,
    history_position: usize,
    max_history: usize,

    // Operation history (for .history command)
    operation_log: Vec<String>,

    // Variable snapshots: stores complete variable state at each history point
    variable_snapshots: Vec<std::collections::HashMap<String, DataFrame>>,
}

impl Repl {
    pub fn new() -> Result<Self> {
        Ok(Self {
            editor: DefaultEditor::new()
                .map_err(|e| crate::error::DtransformError::ReadlineError(e.to_string()))?,
            executor: Executor::new(),
            current: None,
            history: Vec::new(),
            history_position: 0,
            max_history: 10,
            operation_log: Vec::new(),
            variable_snapshots: Vec::new(),
        })
    }

    pub fn run(&mut self) -> Result<()> {
        println!("{}", "Data Transform REPL v0.1.0".bright_blue().bold());
        println!("Type .help for help, .exit to quit");
        println!(
            "Use .undo/.redo to step through operations\n"
        );

        let mut accumulated_input = String::new();

        loop {
            let prompt = if accumulated_input.is_empty() {
                ">> "
            } else {
                ".. "
            };

            let readline = self.editor.readline(prompt);
            match readline {
                Ok(line) => {
                    // Check if line continues (ends with pipe)
                    let trimmed = line.trim();

                    if trimmed.is_empty() && accumulated_input.is_empty() {
                        continue;
                    }

                    // Append to accumulated input
                    if !accumulated_input.is_empty() {
                        accumulated_input.push('\n');
                    }
                    accumulated_input.push_str(&line);

                    // Check if we should continue reading (line ends with |)
                    if trimmed.ends_with('|') {
                        continue;
                    }

                    // We have a complete statement, process it
                    let _ = self.editor.add_history_entry(accumulated_input.as_str());

                    // Normalize multi-line input: replace newlines with spaces
                    let normalized = accumulated_input.replace('\n', " ");

                    if let Err(e) = self.handle_input(&normalized) {
                        eprintln!("{}: {}", "Error".red().bold(), e.display_friendly());
                    }

                    // Reset for next statement
                    accumulated_input.clear();
                }
                Err(ReadlineError::Interrupted) => {
                    println!("^C");
                    accumulated_input.clear();
                    continue;
                }
                Err(ReadlineError::Eof) => {
                    println!("Goodbye!");
                    break;
                }
                Err(err) => {
                    eprintln!("Error: {:?}", err);
                    break;
                }
            }
        }
        Ok(())
    }

    fn handle_input(&mut self, input: &str) -> Result<()> {
        // Handle special commands
        if input.starts_with('.') {
            return self.handle_command(input);
        }

        // Parse statement (could be assignment or pipeline)
        let statement = parse(input)?;
        let operation_desc = self.describe_statement(&statement);

        match statement {
            Statement::Assignment { name, pipeline } => {
                // Execute pipeline
                let result = self.executor.execute_pipeline(pipeline)?;

                // Store in executor's variable map
                self.executor.set_variable(name.clone(), result.clone());

                // Also set as current for _
                self.current = Some(result.clone());
                self.save_to_history(Some(name.clone()));

                self.operation_log.push(format!("{} = ...", name));

                println!(
                    "{}: {} ({} rows × {} cols)",
                    "Stored".green(),
                    name,
                    result.height(),
                    result.width()
                );
                self.preview_result(&result);
            }
            Statement::Pipeline(pipeline) => {
                // If pipeline has no source, use current table
                let has_source = pipeline.source.is_some();
                let pipeline_to_execute = if !has_source {
                    // Use current table as source
                    if let Some(ref current_df) = self.current {
                        // Create a temporary variable for the current table
                        self.executor.set_variable("_".to_string(), current_df.clone());

                        let mut modified_pipeline = pipeline;
                        modified_pipeline.source = Some(crate::parser::ast::Source::Variable("_".to_string()));
                        modified_pipeline
                    } else {
                        pipeline
                    }
                } else {
                    pipeline
                };

                // Execute pipeline
                let result = self.executor.execute_pipeline(pipeline_to_execute)?;

                // Save to history for undo
                self.current = Some(result.clone());
                self.save_to_history(None);

                self.operation_log.push(operation_desc);

                // Preview
                self.preview_result(&result);
            }
        }

        Ok(())
    }

    fn describe_statement(&self, statement: &Statement) -> String {
        match statement {
            Statement::Assignment { name, .. } => format!("{} = ...", name),
            Statement::Pipeline(pipeline) => {
                if pipeline.operations.is_empty() {
                    "read(...)".to_string()
                } else {
                    format!("{} operation(s)", pipeline.operations.len())
                }
            }
        }
    }

    fn save_to_history(&mut self, _variable_name: Option<String>) {
        if let Some(ref current) = self.current {
            // Truncate future if we're in the middle of history
            self.history.truncate(self.history_position);
            self.variable_snapshots.truncate(self.history_position);

            // Save current dataframe state
            self.history.push(current.clone());

            // Save complete variable snapshot
            let snapshot = self.executor.get_all_variables();
            self.variable_snapshots.push(snapshot);

            // Limit history size
            if self.history.len() > self.max_history {
                self.history.remove(0);
                self.variable_snapshots.remove(0);
            } else {
                self.history_position += 1;
            }
        }
    }

    fn handle_command(&mut self, cmd: &str) -> Result<()> {
        let parts: Vec<&str> = cmd.split_whitespace().collect();

        match parts[0] {
            ".help" => self.show_help(),
            ".exit" | ".quit" => std::process::exit(0),
            ".schema" => self.show_schema()?,
            ".undo" => {
                let n = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(1);
                self.undo(n)?;
            }
            ".redo" => {
                let n = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(1);
                self.redo(n)?;
            }
            ".history" => self.show_history(),
            ".vars" | ".variables" => self.show_variables(),
            ".clear" => self.clear(),
            _ => println!("Unknown command: {}. Type .help for help.", parts[0]),
        }
        Ok(())
    }

    fn undo(&mut self, n: usize) -> Result<()> {
        if self.history_position == 0 {
            return Err(crate::error::DtransformError::InvalidOperation(
                "No more history to undo".to_string(),
            ));
        }

        let steps = n.min(self.history_position);
        let new_position = self.history_position - steps;

        self.history_position = new_position;

        // Restore dataframe state
        self.current = if self.history_position == 0 {
            None
        } else {
            Some(self.history[self.history_position - 1].clone())
        };

        // Restore variable snapshot
        if self.history_position > 0 {
            let snapshot = self.variable_snapshots[self.history_position - 1].clone();
            self.executor.restore_variables(snapshot);
        } else {
            // At position 0, clear all variables
            self.executor.restore_variables(HashMap::new());
        }

        println!("{} {} step(s)", "Undid".yellow(), steps);

        if let Some(ref df) = self.current {
            self.preview_result(df);
        }

        Ok(())
    }

    fn redo(&mut self, n: usize) -> Result<()> {
        if self.history_position >= self.history.len() {
            return Err(crate::error::DtransformError::InvalidOperation(
                "No more history to redo".to_string(),
            ));
        }

        let steps = n.min(self.history.len() - self.history_position);
        self.history_position += steps;

        // Restore dataframe state
        self.current = Some(self.history[self.history_position - 1].clone());

        // Restore variable snapshot
        let snapshot = self.variable_snapshots[self.history_position - 1].clone();
        self.executor.restore_variables(snapshot);

        println!("{} {} step(s)", "Redid".yellow(), steps);

        if let Some(ref df) = self.current {
            self.preview_result(df);
        }

        Ok(())
    }

    fn show_history(&self) {
        println!("{}", "Operation History:".bright_blue());
        for (i, op) in self.operation_log.iter().enumerate() {
            let marker = if i == self.history_position - 1 {
                " ← current"
            } else {
                ""
            };
            println!("  {}. {}{}", i + 1, op, marker.green());
        }

        if self.operation_log.is_empty() {
            println!("  (no operations yet)");
        }
    }

    fn show_variables(&self) {
        println!("{}", "Stored Variables:".bright_blue());
        let vars = self.executor.list_variables();

        if vars.is_empty() {
            println!("  (no variables stored)");
        } else {
            for name in vars {
                if let Some(df) = self.executor.get_variable(&name) {
                    println!(
                        "  {} → {} rows × {} cols",
                        name,
                        df.height(),
                        df.width()
                    );
                }
            }
        }
    }

    fn clear(&mut self) {
        self.current = None;
        self.history.clear();
        self.history_position = 0;
        self.operation_log.clear();
        self.variable_snapshots.clear();
        println!("{}", "Cleared current table and history".yellow());
    }

    fn show_help(&self) {
        println!("{}", "Available commands:".bright_blue());
        println!("  .help          - Show this help");
        println!("  .exit          - Exit REPL");
        println!("  .schema        - Show current table schema");
        println!("  .undo [n]      - Undo last n operations (default: 1)");
        println!("  .redo [n]      - Redo last n operations (default: 1)");
        println!("  .history       - Show operation history");
        println!("  .vars          - Show stored variables");
        println!("  .clear         - Clear current table and history");
        println!("\n{}", "Multi-line statements:".bright_blue());
        println!("  Lines ending with | continue to the next line");
        println!("  The prompt changes to .. for continuation");
        println!("  Example:");
        println!("    >> data = read('data.csv') |");
        println!("    .. filter(price > 100) |");
        println!("    .. select(product, quantity)");
        println!("\n{}", "Example usage:".bright_blue());
        println!("  data = read('data.csv')");
        println!("  data | select($1, $2) | filter(age > 25)");
        println!("  .undo 2");
        println!("\n{}", "Quick reference:".bright_blue());
        println!("  Pipe operations:        read('file.csv') | select($1,$2) | filter(age > 25)");
        println!("  Rename columns:         rename(old_name -> new_name)");
        println!("  Bulk rename:            rename_all(lowercase)");
        println!("  Smart selection:        select(re('^Sales_'))  # regex");
        println!("                          select(types(Number))  # by type");
        println!("  String operations:      mutate(email = email.lower())");
    }

    fn show_schema(&self) -> Result<()> {
        if let Some(ref df) = self.current {
            println!("{}", "Schema:".bright_blue());
            let schema = df.schema();

            for (i, (name, field)) in schema.iter().enumerate() {
                println!("  {}. {} ({})", i + 1, name, field);
            }

            println!(
                "\n{} rows × {} columns",
                df.height(),
                df.width()
            );
        } else {
            println!("No table loaded. Use read() to load data or a variable name.");
        }
        Ok(())
    }

    fn preview_result(&self, df: &DataFrame) {
        let rows = df.height();
        let cols = df.width();

        println!(
            "\n{}",
            format!("[Table: {} rows × {} cols]", rows, cols).bright_green()
        );

        // Show first few rows
        let preview = df.head(Some(5));
        println!("{}", preview);

        if rows > 5 {
            println!("... {} more rows", rows - 5);
        }
        println!();
    }
}
