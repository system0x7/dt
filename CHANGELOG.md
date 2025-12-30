# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.1] - 2025-12-30

### Fixed
- Allow mutating new columns using `$N` positional notation (e.g., `mutate($2 = expr)` now creates `column_2` when the DataFrame has fewer than 2 columns)

## [0.1.0] - 2025-12-29

### Added
- Initial release of `dt` (data-transform)
- Interactive REPL mode for data transformations
- Support for CSV, TSV, JSON, and Parquet formats
- Core operations: filter, mutate, select, sort, distinct, rename
- Multi-file operations with lookup support
- String operations: split, replace
- Column selection by name or position
- Auto-detection of file formats from extensions

### Features
- Polars-powered parallel processing
- Type-aware operations
- Query optimization and lazy evaluation
- Readable pipeline syntax
- REPL commands: .help, .schema, .vars, .history, .undo, .clear, .exit
