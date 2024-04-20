use crate::domain::Agg;
use std::{cmp::Ordering, collections::HashSet, hash::Hash};

pub(crate) fn cmp_length_desc(a: &str, b: &str) -> Ordering {
    let a_len = a.chars().count();
    let b_len = b.chars().count();
    b_len.cmp(&a_len)
}

pub(crate) fn has_duplicates<T: Eq + Hash>(items: &[T]) -> bool {
    let mut seen = HashSet::new();
    for item in items {
        if !seen.insert(item) {
            return true;
        }
    }
    false
}

pub(crate) fn starts_with_agg(column: &str) -> bool {
    Agg::values()
        .iter()
        .map(|agg| format!("{}_", agg))
        .any(|s| column.starts_with(&s))
        || column.starts_with("countstar")
}
