use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum ColumnType {
    Number,
    Boolean,
    Text,
    Time,
    Others,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct SqlSchema {
    pub(crate) db_id: String,
    pub(crate) table_names: Vec<String>,
    pub(crate) column_names: Vec<String>,
    pub(crate) column_types: Vec<ColumnType>,
    pub(crate) column_to_table: Vec<usize>,
    pub(crate) table_to_columns: HashMap<String, Vec<usize>>,
    pub(crate) foreign_keys: Vec<(usize, usize)>,
    pub(crate) primary_keys: Vec<usize>,
}

impl SqlSchema {
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        db_id: String,
        table_names: Vec<String>,
        column_names: Vec<String>,
        column_types: Vec<ColumnType>,
        column_to_table: Vec<usize>,
        table_to_columns: HashMap<String, Vec<usize>>,
        foreign_keys: Vec<(usize, usize)>,
        primary_keys: Vec<usize>,
    ) -> Self {
        Self {
            db_id,
            table_names,
            column_names,
            column_types,
            column_to_table,
            table_to_columns,
            foreign_keys,
            primary_keys,
        }
    }
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Ord, Eq)]
pub(crate) enum KeyType {
    PrimaryKey { table: String },
    ForeignKey { table: String },
}

#[derive(Clone, Debug)]
pub(crate) enum Column {
    Dummy,
    Plain {
        name: String,
        typ: ColumnType,
        keys: Vec<KeyType>,
    },
    Aliased {
        name: String,
        typ: ColumnType,
        keys: Vec<KeyType>,
    },
}

impl Column {
    pub(crate) fn name(&self) -> &str {
        match self {
            Column::Dummy => "1 AS One",
            Column::Plain { name, .. } | Column::Aliased { name, .. } => name,
        }
    }

    pub(crate) fn typ(&self) -> &ColumnType {
        match self {
            Column::Dummy => &ColumnType::Number,
            Column::Plain { typ, .. } | Column::Aliased { typ, .. } => typ,
        }
    }

    pub(crate) fn keys(&self) -> &[KeyType] {
        match self {
            Column::Dummy => &[],
            Column::Plain { keys, .. } | Column::Aliased { keys, .. } => keys,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum Table {
    Named { name: String, columns: Vec<Column> },
    Indexed { idx: usize, columns: Vec<Column> },
}

impl Table {
    pub(crate) fn columns(&self) -> &[Column] {
        match self {
            Table::Named { columns, .. } => columns,
            Table::Indexed { columns, .. } => columns,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Comparable {
    Number(f64),
    Str(String),
    Boolean(bool),
    Null,
    Column(String),
}

#[derive(Debug, PartialEq)]
pub(crate) enum Comparison {
    Equal(Comparable, Comparable),
    NotEqual(Comparable, Comparable),
    GreaterThan(Comparable, Comparable),
    GreaterThanOrEqual(Comparable, Comparable),
    LessThan(Comparable, Comparable),
    LessThanOrEqual(Comparable, Comparable),
    Is(Comparable, Comparable),
    IsNot(Comparable, Comparable),
    Like(Comparable, Comparable),
    NotLike(Comparable, Comparable),
}

impl Comparison {
    pub(crate) fn from_string(op: &str, lhs: Comparable, rhs: Comparable) -> Comparison {
        use Comparison::*;
        match op {
            "=" => Equal(lhs, rhs),
            "<>" => NotEqual(lhs, rhs),
            ">" => GreaterThan(lhs, rhs),
            ">=" => GreaterThanOrEqual(lhs, rhs),
            "<" => LessThan(lhs, rhs),
            "<=" => LessThanOrEqual(lhs, rhs),
            "IS" => Is(lhs, rhs),
            "IS NOT" => IsNot(lhs, rhs),
            "LIKE" => Like(lhs, rhs),
            "NOT LIKE" => NotLike(lhs, rhs),
            _ => panic!("Operation \"{}\" is not supported.", op),
        }
    }
}

#[derive(Debug, PartialEq)]
pub(crate) enum Predicate {
    Single {
        comparison: Comparison,
    },
    And {
        lhs: Box<Predicate>,
        rhs: Box<Predicate>,
    },
    Or {
        lhs: Box<Predicate>,
        rhs: Box<Predicate>,
    },
}

#[derive(Debug, PartialEq)]
pub(crate) enum ExceptOperator {
    Predicate(Predicate),
    ExceptColum(String),
}

#[derive(Debug, PartialEq)]
pub(crate) enum Operation {
    Aggregate {
        input: usize,
        group_by: Vec<String>,
    },
    Except {
        inputs: Vec<usize>,
        operator: ExceptOperator,
        is_distinct: bool,
    },
    Filter {
        input: usize,
        predicate: Option<Predicate>,
        is_distinct: bool,
    },
    Intersect {
        inputs: Vec<usize>,
        predicate: Option<Predicate>,
        is_distinct: bool,
    },
    Join {
        inputs: Vec<usize>,
        predicate: Option<Predicate>,
        is_distinct: bool,
    },
    Scan {
        table: String,
        predicate: Option<Predicate>,
        is_distinct: bool,
    },
    Top {
        input: usize,
        rows: usize,
    },
    Sort {
        input: usize,
        order_by: Vec<String>,
        is_distinct: bool,
    },
    TopSort {
        input: usize,
        rows: usize,
        order_by: Vec<String>,
        with_ties: bool,
    },
    Union {
        inputs: Vec<usize>,
    },
}

#[derive(Clone)]
pub(crate) enum Agg {
    Sum,
    Min,
    Max,
    Count,
    Average,
}

impl Agg {
    pub(crate) fn values() -> Vec<Agg> {
        use Agg::*;

        vec![Sum, Min, Max, Count, Average]
    }
}

impl std::fmt::Display for Agg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Agg::Sum => write!(f, "Sum"),
            Agg::Min => write!(f, "Min"),
            Agg::Max => write!(f, "Max"),
            Agg::Count => write!(f, "Count"),
            Agg::Average => write!(f, "Avg"),
        }
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct Line {
    pub(crate) idx: usize,
    pub(crate) operation: Operation,
}

pub(crate) type Qpl = Vec<Line>;

#[derive(Clone, Debug, Default)]
pub(crate) struct QplState {
    pub(crate) current_idx: usize,
    pub(crate) seen: HashSet<usize>,
    pub(crate) idx_to_table: HashMap<usize, Table>,
}

#[derive(Clone, Debug)]
pub(crate) struct QplEnvironment {
    pub(crate) state: QplState,
    pub(crate) schema: Option<SqlSchema>,
}
