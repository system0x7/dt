# Data Transform (dt) - Quick Reference

## I/O Operations

### read(path, ...)
Read data from file. Auto-detects delimiter and whitespace handling.

**Auto-detection:**
- Delimiter: Scans file to detect comma, tab, space, pipe, or semicolon
- Whitespace trimming: Enabled when leading/trailing spaces or multiple consecutive spaces found
- Most files work with minimal parameters

**Parameters (all optional):**
- `header=false` - File has no header row (default: true)
- `delimiter=' '` - Override delimiter (use when auto-detection fails)
- `trim_whitespace=true/false` - Override trimming (rarely needed; auto-detection handles this)
- `skip_rows=N` - Skip N lines before parsing

**Examples:**
```bash
read('data.csv')                    # Auto-detects everything
read('data.ind', header=false)      # Auto-detects delimiter=' ', trim=true
read('file.txt', delimiter='\t')    # Force tab delimiter
read('messy.csv', skip_rows=2)      # Skip header lines
```

### write(path, ...)
Write data to file.

**Parameters:**
- `delimiter=' '` - Output field separator
- `header=false` - Don't write header row

**Examples:**
```bash
write('output.csv')
write('data.txt', delimiter=' ', header=false)
```

## Column Selection

### select(cols...)
Keep only specified columns.

```bash
select($1, $2, $3)        # By position (1-based, must use $ prefix)
select($1..$5)            # Columns 1-5 (range)
select($1..$5, $10..$15)  # Multiple ranges
select(name, age)         # By name
select(re('^Sales_'))     # Regex pattern
select(types(Number))     # All numeric columns

# With aliases (rename during selection)
select($1 as id, $3 as population)
select(name as full_name)
```

### drop(cols...)
Remove columns.

```bash
drop(column_3)
drop($1, $2, $3)
drop($3..$7)              # Drop columns 3-7
```

## Filtering & Sorting

### filter(condition)
Keep rows matching condition.

```bash
filter(age > 30)
filter(name == "Alice")
filter(salary > 50000 and department == "Engineering")
filter($3 in want)        # Column in variable
```

**Operators:** `>`, `<`, `>=`, `<=`, `==`, `!=`, `and`, `or`, `in`

### sort(column [desc])
Sort by column(s).

```bash
sort(age)
sort(salary desc)
sort($3 desc)
```

### distinct([cols...])
Remove duplicate rows.

```bash
distinct()                # All columns
distinct(user_id)         # By specific column
distinct($1, $2)          # By multiple columns
```

## Row Limits

```bash
take(10)                  # First 10 rows
skip(5)                   # Skip first 5 rows
slice(10, 20)             # Rows 10-19 (0-based, exclusive end)
```

## Transformations

### mutate(assignments...)
Add or modify columns. If a column name already exists, it's updated in place. Otherwise, a new column is added.

```bash
# Numeric operations
mutate(total = price * quantity)
mutate(age_group = age / 10)

# String concatenation with +
mutate(full_name = first_name + " " + last_name)
mutate(id = 'prefix:' + sample_id)
mutate(compound_id = $1 + ':' + $2)
```

### rename(old -> new, ...)
Rename columns.

```bash
rename(old_name -> new_name)
rename($1 -> id, $2 -> name)
```

### rename_all(strategy)
Bulk rename columns.

```bash
# Replace pattern in all column names
rename_all(replace(' ', '_'))
rename_all(replace('_', '-'))
rename_all(replace('old', 'new'))

# Sequential numbering with prefix
rename_all('col' + 1..5)      # col1, col2, col3, col4, col5
rename_all('V' + 1..3)        # V1, V2, V3
rename_all('var' + 10..12)    # var10, var11, var12
```

**Important:** For sequential numbering, the range must exactly match the number of columns. If mismatched, you'll get an error suggesting to use `select()` first to adjust column count (should prevent header corruption).

## String Operations

### String Concatenation

Use the `+` operator to concatenate strings:

```bash
# Concatenate columns
mutate(full_name = first_name + " " + last_name)

# Concatenate with literals
mutate(id = 'prefix:' + sample_id)

# Concatenate multiple columns
mutate(compound_id = $1 + ':' + $2)
mutate(full_id = 'sample:' + iid + '_' + pop)

# Numbers are automatically converted to strings
mutate(label = 'ID_' + user_id)  # user_id can be numeric
```

**Note:** The `+` operator works for both numeric addition and string concatenation. When both operands are strings, they are concatenated. When both are numbers, they are added.

### replace() Function

Replace text within string columns using literal strings or regex patterns:

```bash
# Basic literal string replacement
mutate(clean = replace(text, 'old', 'new'))

# Remove a prefix by replacing with empty string
mutate(clean_id = replace(id, 'DRAFT:', ''))

# Works with positional columns
mutate(clean = replace($1, 'prefix_', ''))
mutate(updated = replace($4, 'old', 'new'))

# In-place transformation using same column
mutate($3 = replace($3, 'old', 'new'))

# Regex patterns using re('pattern')
mutate(label = replace(label, re('\.(AG|DG|HO|SG)$'), ''))
mutate(clean = replace(text, re('^prefix_'), ''))
mutate(id = replace(id, re('[^a-zA-Z0-9]'), '_'))

# Combining with other functions
mutate($1 = replace(split($1, ':')[0], re('\s+'), '_'))
```

**Syntax:** `replace(text_column, pattern, replacement)`
- **text_column**: Column or positional reference containing strings
- **pattern**: Either a literal string `'text'` or regex pattern `re('pattern')`
- **replacement**: String to replace with (can be empty string '')

## Regex Patterns in replace()

Use `re('pattern')` to enable regex matching. Without `re()`, replace() does literal string matching.

```bash
# Literal matching (exact string)
replace(text, 'old', 'new')

# Regex matching (pattern-based)
replace(text, re('\\.(AG|DG|HO|SG)$'), '')
```

### When to Use Regex
- **Literal**: When you know the exact text to replace
- **Regex**: When you need patterns (word boundaries, multiple alternatives, positions)

**Note:** `re()` also works in `select()` to match column names: `select(re('^Sales_'))`

### Special Regex Symbols

**Position Anchors:**
- `^` - Start of string: `re('^red')` matches "red car" but not "my red car"
- `$` - End of string: `re('red$')` matches "I like red" but not "red car"
- `\\b` - Word boundary: `re('\\bred\\b')` matches "red car" but not "scared"

**Character Classes:**
- `.` - Any single character: `re('r.d')` matches "red", "rod", "r3d"
- `[abc]` - Any one character in brackets: `re('[Rr]ed')` matches "Red" or "red"
- `[^abc]` - Any character NOT in brackets: `re('[^a-z]')` matches non-lowercase
- `\\d` - Any digit (same as `[0-9]`)
- `\\w` - Any word character (letter, digit, underscore)
- `\\s` - Any whitespace (space, tab, newline)

**Quantifiers:**
- `*` - Zero or more: `re('red*')` matches "re", "red", "redd"
- `+` - One or more: `re('red+')` matches "red", "redd", but not "re"
- `?` - Zero or one (optional): `re('colou?r')` matches "color" and "colour"
- `{n}` - Exactly n times: `re('[0-9]{3}')` matches exactly 3 digits
- `{n,m}` - Between n and m: `re('[0-9]{2,4}')` matches 2-4 digits

**Grouping and Alternation:**
- `|` - OR: `re('(red|blue|green)')` matches any of those colors
- `()` - Grouping: `re('(red)+')` matches "red", "redred", "redredred"

**Escaping Special Characters:**

To match special characters literally, prefix with `\`:
- `\\.` - Literal dot (otherwise `.` = any character)
- `\\$` - Literal dollar sign (otherwise `$` = end of string)
- `\\(` - Literal parenthesis
- `\\[` - Literal bracket
- `\\\\` - Literal backslash

**Common Patterns:**
```bash
# Replace 'red' only as whole word (not in 'scared')
replace(text, re('\\bred\\b'), 'blue')

# Remove file extensions (.txt, .csv, etc.)
replace(filename, re('\\.[a-z]+$'), '')

# Replace multiple spaces with single space
replace(text, re('\\s+'), ' ')

# Replace all non-alphanumeric with underscore
replace(text, re('[^a-zA-Z0-9]'), '_')

# Remove leading/trailing whitespace
replace(text, re('^\\s+|\\s+$'), '')
```

### split() Function

Extract parts from delimited strings (0-indexed):

```bash
# Extract domain from email
mutate(domain = split(email, '@')[1])

# Extract multiple parts
mutate(fid = split($1, ':')[0], iid = split($1, ':')[1])

# Out of bounds returns null (no error)
mutate(third = split(text, ':')[2])  # null if only 2 parts

# Empty strings are preserved
# "A::C" split by ':' → ["A", "", "C"]
mutate(middle = split(id, ':')[1])
```

**Note**: `split()` must be followed by `[index]` - standalone split without index is not allowed.

### lookup() Function

Look up values from a reference table:

```bash
# Syntax: lookup(table, key_expr, on='match_column', return='return_column')
# - table: lookup table variable
# - key_expr: column from current data to match (sample_id, $1, etc.)
# - on: column in lookup table to match against
# - return: column from lookup table to return
# Both on= and return= accept column names or $N positional notation

# Basic lookup - match sample_id against 'id' column, return 'label'
mutate(label = lookup(labels, sample_id, on='id', return='label'))

# Using $N notation for on/return parameters
mutate(label = lookup(labels, sample_id, on=$1, return=$2))
mutate(pop = lookup(labels, $1, on=$1, return=$3))

# Lookup with positional columns in key expression
mutate(label = lookup(labels, $1, on='id', return='label'))

# Lookup with complex expressions (split, etc.)
mutate(label = lookup(labels, split($1, ':')[1], on='id', return='label'))
mutate(label = lookup(labels, iids, on=$1, return=$3))
```

**How it works:**
- **First parameter**: variable name of the lookup table
- **Second parameter**: expression to evaluate as the lookup key
- **on='column'** or **on=$N**: field in lookup table to match against (name or position)
- **return='column'** or **return=$N**: field to return from the lookup table (name or position)
- Returns the value from the specified field, or **null** if no match is found

**Example:**
```bash
# Create lookup table with id, label, population, region columns
labels = read('labels.csv')

# Load data and enrich with lookups (single-line)
data = read('samples.csv') | mutate(label = lookup(labels, sample_id, on='id', return='label'), population = lookup(labels, sample_id, on='id', return='population'), region = lookup(labels, sample_id, on='id', return='region'))

# Or split at pipe for script files
data = read('samples.csv') |
	mutate(label = lookup(labels, sample_id, on='id', return='label'), population = lookup(labels, sample_id, on='id', return='population'), region = lookup(labels, sample_id, on='id', return='region'))

# Result: S001 -> Sample_A, 1000, North
#         S002 -> Sample_B, 2000, South
#         S999 -> null, null, null  (not found)
```

**Handling missing values:**
- When a key is not found in the lookup table, the function returns **null**
- No error is thrown; the pipeline continues with null values
- You can filter out nulls afterward if needed: `filter(label != null)`

## Variables

Store intermediate results:

```bash
want = read('pops.txt', header=false)
keep = read('data.ind', delimiter=' ', header=false) | filter($3 in want)
```

## REPL Commands

- `.help` - Show help
- `.schema` - Show current table schema
- `.vars` - Show stored variables
- `.undo [n]` - Undo last n operations
- `.redo [n]` - Redo last n operations
- `.history` - Show operation history
- `.clear` - Clear current table and history
- `.exit` - Exit REPL

## Command Line Usage

```bash
# Interactive REPL
dt

# One-liner
dt "read('data.csv') | filter(age > 25) | write('output.csv')"

# From script file
dt -f script.dt

# Specify output file
dt -o result.csv "read('input.csv') | filter(age > 30)"
```

## Supported File Formats

**Auto-detected:**
- `.csv` - Comma-separated
- `.tsv` - Tab-separated
- `.json` - JSON
- `.parquet` - Parquet

**Custom delimited:**
- Any extension - specify `delimiter` parameter
