use super::{
    shared::{
        boolean, get_table_from_indexed_outputs, indexed_column, input_ids, null, number,
        predicate_wrapper, spaced_comparison_op, string, Stream,
    },
    utils::has_duplicates,
};
use crate::domain::{
    Column, ColumnType, Comparable, Comparison, ExceptOperator, KeyType, Operation, Predicate,
    Table,
};
use std::collections::{HashMap, HashSet};
use winnow::{
    ascii::multispace0,
    combinator::{alt, empty, fail, opt, separated, separated_foldl1, todo},
    error::ParserError,
    PResult, Parser,
};

pub(crate) fn union<'i, E: ParserError<Stream<'i>>>(
    input: &mut Stream<'i>,
) -> PResult<Operation, E> {
    "Union ".parse_next(input)?;
    let inputs = input_ids.parse_next(input)?;
    if inputs.len() != 2 {
        return fail.parse_next(input);
    }
    "Output [ ".parse_next(input)?;
    let outs_with_index: Vec<(usize, String)> =
        separated(1.., indexed_column(&inputs), (multispace0, ", ")).parse_next(input)?;
    let idx_to_table = &input.state.state.idx_to_table;
    if !validate_output(&inputs, &outs_with_index, idx_to_table) {
        return fail.parse_next(input);
    }
    let output_table = get_table_from_indexed_outputs(outs_with_index).parse_next(input)?;
    let state = &mut input.state.state;
    state.idx_to_table.insert(state.current_idx, output_table);
    " ]".parse_next(input)?;
    Ok(Operation::Union { inputs })
}

fn validate_output(
    inputs: &[usize],
    outs: &[(usize, String)],
    idx_to_table: &HashMap<usize, Table>,
) -> bool {
    let prev_columns = inputs
        .iter()
        .map(|i| &idx_to_table[i])
        .flat_map(|t| t.columns())
        .map(|c| c.name())
        .collect::<HashSet<_>>();
    let columns = outs.iter().map(|(_, out)| out).collect::<Vec<_>>();
    let is_dummy = columns == vec!["1 AS One"];
    let is_subset_of_prev = columns.iter().all(|&c| prev_columns.contains(c.as_str()));

    !has_duplicates(outs) && (is_subset_of_prev || is_dummy)
}
