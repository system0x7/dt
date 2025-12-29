use clap::Parser;
use data_transform::{error::Result, Executor, Repl};
use polars::prelude::*;

#[derive(Parser)]
#[command(name = "dt")]
#[command(about = "Data Transform - Simple, fast data transformation", long_about = None)]
#[command(version)]
struct Cli {
    /// Pipeline to execute
    #[arg(value_name = "PIPELINE")]
    pipeline: Option<String>,

    /// Read pipeline from file
    #[arg(short, long, value_name = "FILE")]
    file: Option<String>,

    /// Start interactive REPL
    #[arg(short, long)]
    interactive: bool,

    /// Output file (default: stdout)
    #[arg(short, long, value_name = "FILE")]
    output: Option<String>,

    /// Verbose output
    #[arg(short, long)]
    verbose: bool,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    if cli.interactive || (cli.pipeline.is_none() && cli.file.is_none()) {
        // Start REPL
        let mut repl = Repl::new()?;
        repl.run()?;
    } else if let Some(pipeline_str) = cli.pipeline {
        // Execute inline pipeline
        execute_pipeline(&pipeline_str, cli.output, cli.verbose)?;
    } else if let Some(file_path) = cli.file {
        // Execute pipeline from file
        let pipeline_str = std::fs::read_to_string(file_path)?;
        execute_pipeline(&pipeline_str, cli.output, cli.verbose)?;
    }

    Ok(())
}

fn execute_pipeline(pipeline_str: &str, output: Option<String>, verbose: bool) -> Result<()> {
    let program = data_transform::parse_program(pipeline_str)?;

    if verbose {
        println!("Executing {} statement(s)", program.statements.len());
    }

    let mut executor = Executor::new();
    let result = executor.execute_program(program)?;

    if let Some(df) = result {
        if let Some(output_path) = output {
            // Write to file
            let mut file = std::fs::File::create(output_path)?;
            CsvWriter::new(&mut file).finish(&mut df.clone())?;

            if verbose {
                println!(
                    "Output written: {} rows × {} cols",
                    df.height(),
                    df.width()
                );
            }
        } else {
            // Write to stdout
            println!("{}", df);
        }
    } else if verbose {
        println!("No output (script only performed assignments)");
    }

    Ok(())
}
