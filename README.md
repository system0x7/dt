# Data Transform (dt)

A fast, readable data transformation tool for working with tabular data. Built with Rust and Polars.

## Why dt?

**Clearer than pandas, faster than awk, zero setup.**

- **Readable syntax**: Named operations and clear pipelines
- **Polars-powered**: Parallel processing, optimized queries, columnar operations
- **Single binary**: No Python environments, no dependencies to manage
- **Interactive REPL**: See results after each transformation
- **Multi-file operations**: Clean syntax for joins and lookups

## Installation

```bash
# Shell installer (macOS/Linux)
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/system0x7/dt/releases/latest/download/data-transform-installer.sh | sh

# Cargo
cargo install data-transform

# From source
cargo install --git https://github.com/system0x7/dt
```

## Quick Start

### Interactive REPL

```bash
$ dt
>> data = read('sales.csv')
Stored: data (7 rows × 4 cols)
[Table: 7 rows × 4 cols]
shape: (5, 4)
┌──────────┬─────────────┬────────┬──────────┐
│ product  ┆ category    ┆ price  ┆ quantity │
│ ---      ┆ ---         ┆ ---    ┆ ---      │
│ str      ┆ str         ┆ f64    ┆ i64      │
╞══════════╪═════════════╪════════╪══════════╡
│ Laptop   ┆ Electronics ┆ 899.99 ┆ 5        │
│ Mouse    ┆ Electronics ┆ 24.99  ┆ 120      │
│ Desk     ┆ Furniture   ┆ 299.0  ┆ 8        │
│ Chair    ┆ Furniture   ┆ 159.99 ┆ 15       │
│ Keyboard ┆ Electronics ┆ 79.99  ┆ 45       │
└──────────┴─────────────┴────────┴──────────┘
... 2 more rows

>> data = data | filter(price > 100)
Stored: data (5 rows × 4 cols)
[Table: 5 rows × 4 cols]
shape: (5, 4)
┌───────────┬─────────────┬────────┬──────────┐
│ product   ┆ category    ┆ price  ┆ quantity │
│ ---       ┆ ---         ┆ ---    ┆ ---      │
│ str       ┆ str         ┆ f64    ┆ i64      │
╞═══════════╪═════════════╪════════╪══════════╡
│ Laptop    ┆ Electronics ┆ 899.99 ┆ 5        │
│ Desk      ┆ Furniture   ┆ 299.0  ┆ 8        │
│ Chair     ┆ Furniture   ┆ 159.99 ┆ 15       │
│ Monitor   ┆ Electronics ┆ 349.99 ┆ 12       │
│ Bookshelf ┆ Furniture   ┆ 189.0  ┆ 6        │
└───────────┴─────────────┴────────┴──────────┘

>> data = data | mutate(total = price * quantity)
Stored: data (5 rows × 5 cols)
[Table: 5 rows × 5 cols]
shape: (5, 5)
┌───────────┬─────────────┬────────┬──────────┬─────────┐
│ product   ┆ category    ┆ price  ┆ quantity ┆ total   │
│ ---       ┆ ---         ┆ ---    ┆ ---      ┆ ---     │
│ str       ┆ str         ┆ f64    ┆ i64      ┆ f64     │
╞═══════════╪═════════════╪════════╪══════════╪═════════╡
│ Laptop    ┆ Electronics ┆ 899.99 ┆ 5        ┆ 4499.95 │
│ Desk      ┆ Furniture   ┆ 299.0  ┆ 8        ┆ 2392.0  │
│ Chair     ┆ Furniture   ┆ 159.99 ┆ 15       ┆ 2399.85 │
│ Monitor   ┆ Electronics ┆ 349.99 ┆ 12       ┆ 4199.88 │
│ Bookshelf ┆ Furniture   ┆ 189.0  ┆ 6        ┆ 1134.0  │
└───────────┴─────────────┴────────┴──────────┴─────────┘

>> data = data | select(product, category, total)
Stored: data (5 rows × 3 cols)
[Table: 5 rows × 3 cols]
shape: (5, 3)
┌───────────┬─────────────┬─────────┐
│ product   ┆ category    ┆ total   │
│ ---       ┆ ---         ┆ ---     │
│ str       ┆ str         ┆ f64     │
╞═══════════╪═════════════╪═════════╡
│ Laptop    ┆ Electronics ┆ 4499.95 │
│ Desk      ┆ Furniture   ┆ 2392.0  │
│ Chair     ┆ Furniture   ┆ 2399.85 │
│ Monitor   ┆ Electronics ┆ 4199.88 │
│ Bookshelf ┆ Furniture   ┆ 1134.0  │
└───────────┴─────────────┴─────────┘

>> data | write('revenue.csv')
Written: revenue.csv (5 rows × 3 cols)
```

The REPL shows the table after each operation, letting you verify transformations before saving.

### One-liner
```bash
   dt "read('data.csv') | filter(price > 100) | write('output.csv')"
```

### From script file
```bash
dt -f transform.dt
```

## Example: Multi-file lookup
```bash
# Load reference data
pops = read('pops.txt', header=false)
ref = read('reference.ind', header=false)
fam = read('reference.fam', header=false)

# Filter and join
keep = ref | filter($3 in pops)
result = fam | filter($2 in keep) | select($1, $2)

# Save results
result | write('output.tsv')  # .tsv extension auto-uses tab delimiter
```

## Core Features

### Column Selection
```bash
select($1, $2, $3)           # By position (1-based)
select($1..$5)               # Range (inclusive)
select(name, age, email)     # By name
select($1 as id)             # With renaming
drop($3..$7)                 # Remove columns
```

### Filtering & Sorting
```bash
filter(age > 30)
filter(name == "Alice")
filter($3 in populations)
sort(age desc)
distinct(user_id)
```

### Transformations
```bash
mutate(total = price * quantity)
mutate(full_name = first + " " + last)
mutate(domain = split(email, '@')[1])
mutate(label = lookup(labels, id, on='id', return='name'))
```

### String Operations
```bash
mutate(clean = replace(text, 'old', 'new'))
mutate(parts = split(id, ':')[0])
mutate(combined = $1 + ':' + $2)
```

### Renaming
```bash
rename(old_name -> new_name)
rename_all(replace('_', '-'))
rename_all('PC' + 1..50)     # PC1, PC2, ..., PC50
```

### Multi-file Operations
```bash
# Load reference table
labels = read('labels.csv')

# Lookup values (single-line)
data = read('samples.csv') | mutate(population = lookup(labels, sample_id, on='id', return='pop'), region = lookup(labels, sample_id, on='id', return='region'))

# Or split at pipe boundaries for readability
data = read('samples.csv') |
	mutate(population = lookup(labels, sample_id, on='id', return='pop'), region = lookup(labels, sample_id, on='id', return='region'))
```

## Documentation

See [REFERENCE](REFERENCE.md) for complete syntax and examples.

## Supported Formats

- **JSON** (`.json`) - Structured JSON data
- **Parquet** (`.parquet`) - Columnar format

**Delimited text files** - Delimiter auto-detected for any file:
- `.csv` - Defaults to comma, auto-detects if ambiguous
- `.tsv` - Defaults to tab, auto-detects if ambiguous
- Any other extension (`.txt`, `.dat`, `.psv`, etc.) - Auto-detects delimiter

Auto-detection analyzes file content and identifies: comma, tab, pipe, semicolon, or space.

Override auto-detection if needed:
```bash
read('data.txt', delimiter=' ')
read('data.psv', delimiter='|')
```

## Performance

Built on Polars, dt provides:
- Parallel processing using all CPU cores
- Columnar memory layout for cache efficiency
- Query optimization and lazy evaluation
- Type-aware operations

For typical data transformation tasks, dt is 5-10x faster than awk while being significantly more readable.

## REPL Commands

- `.help` - Show help
- `.schema` - Show current table schema
- `.vars` - Show stored variables
- `.history` - Show operation history
- `.undo [n]` - Undo operations
- `.clear` - Clear current state
- `.exit` - Exit REPL

## License

MIT