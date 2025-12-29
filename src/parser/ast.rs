use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Program {
    pub statements: Vec<Statement>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Statement {
    Assignment { name: String, pipeline: Pipeline },
    Pipeline(Pipeline),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Pipeline {
    pub source: Option<Source>,
    pub operations: Vec<Operation>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Source {
    Read(ReadOp),
    Variable(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Operation {
    Read(ReadOp),
    Variable(String),  // Variable reference (e.g., "data" in "data | filter(...)")
    Write(WriteOp),
    Select(SelectOp),
    Filter(FilterOp),
    Mutate(MutateOp),
    Rename(RenameOp),
    RenameAll(RenameAllOp),
    Sort(SortOp),
    Take(TakeOp),
    Skip(SkipOp),
    Slice(SliceOp),
    Drop(DropOp),
    Distinct(DistinctOp),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReadOp {
    pub path: String,
    pub format: Option<String>,
    pub delimiter: Option<char>,
    pub header: Option<bool>,  // NEW: Whether the file has a header row
    pub skip_rows: Option<usize>,  // NEW: Number of rows to skip before reading
    pub trim_whitespace: Option<bool>,  // NEW: Trim leading/trailing whitespace from each line
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WriteOp {
    pub path: String,
    pub format: Option<String>,
    pub header: Option<bool>,
    pub delimiter: Option<char>,  // NEW: Delimiter character for output
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SelectOp {
    pub selectors: Vec<(ColumnSelector, Option<String>)>, // (selector, optional alias)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ColumnSelector {
    Name(String),
    Index(usize), // 0-based internally, only via $N syntax
    Range(usize, usize), // 0-based internally, only via $N..$M syntax
    Regex(String),
    Type(Vec<DataType>),
    All,
    Except(Box<ColumnSelector>),
    And(Box<ColumnSelector>, Box<ColumnSelector>),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DataType {
    Number,
    String,
    Boolean,
    Date,
    DateTime,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FilterOp {
    pub condition: Expression,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MutateOp {
    pub assignments: Vec<Assignment>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Assignment {
    pub column: AssignmentTarget,
    pub expression: Expression,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AssignmentTarget {
    Name(String),      // Named column: name, column_name
    Position(usize),   // Positional column: $1, $2, etc. (1-based)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RenameOp {
    pub mappings: Vec<(ColumnRef, String)>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RenameAllOp {
    pub strategy: RenameStrategy,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RenameStrategy {
    Replace { old: String, new: String },
    Sequential { prefix: String, start: usize, end: usize },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ColumnRef {
    Name(String),
    Index(usize),      // 0-based index for internal use
    Position(usize),   // 1-based AWK-style ($1, $2, etc.)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SortOp {
    pub columns: Vec<(ColumnRef, bool)>, // (column, descending)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TakeOp {
    pub n: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SkipOp {
    pub n: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SliceOp {
    pub start: usize,
    pub end: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DropOp {
    pub columns: Vec<ColumnSelector>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DistinctOp {
    pub columns: Option<Vec<ColumnSelector>>,  // None = all columns
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expression {
    Literal(Literal),
    Column(ColumnRef),
    List(Vec<Literal>),  // List literal for 'in' operator: ['a', 'b', 'c']
    Variable(String),  // Variable reference (e.g., "want" in "filter($3 in want)")
    BinaryOp {
        left: Box<Expression>,
        op: BinOp,
        right: Box<Expression>,
    },
    MethodCall {
        object: Box<Expression>,
        method: String,
        args: Vec<Expression>,
    },
    Split {
        string: Box<Expression>,
        delimiter: Box<Expression>,
        index: usize,
    },
    Lookup {
        table: String,              // Variable name of the lookup table
        key: Box<Expression>,       // Expression to evaluate as lookup key
        on: LookupField,            // Field in lookup table to match against
        return_field: LookupField,  // Field to return from lookup table
    },
    Replace {
        text: Box<Expression>,      // Expression to perform replacement on
        old: Box<Expression>,       // Pattern to replace
        new: Box<Expression>,       // Replacement text
    },
    Regex(String),  // Regex pattern literal: re('pattern')
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum LookupField {
    Name(String),          // Explicit column name: 'column_name'
    Position(usize),       // Positional column: $1, $2, etc. (1-based)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Literal {
    Number(f64),
    String(String),
    Boolean(bool),
    Null,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Gt,
    Lt,
    Gte,
    Lte,
    Eq,
    Neq,
    And,
    Or,
    In,  // Membership test (value in collection)
}
