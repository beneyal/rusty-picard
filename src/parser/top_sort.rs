use super::{
    shared::{aliased_column, column_name, get_output, input_ids, order_by, Stream},
    utils::has_duplicates,
};
use crate::domain::{Operation, Table};
use std::collections::{HashMap, HashSet};
use winnow::{
    ascii::{dec_uint, multispace0},
    combinator::{alt, empty, fail, separated},
    error::ParserError,
    PResult, Parser,
};

pub(crate) fn top_sort<'i, E: ParserError<Stream<'i>>>(
    input: &mut Stream<'i>,
) -> PResult<Operation, E> {
    "TopSort ".parse_next(input)?;
    let inputs = input_ids.parse_next(input)?;
    if inputs.len() != 1 {
        return fail.parse_next(input);
    }
    let input_idx = inputs[0];
    "Rows [ ".parse_next(input)?;
    let rows = dec_uint.parse_next(input)?;
    " ] OrderBy [ ".parse_next(input)?;
    let obs = separated(1.., order_by(input_idx), (multispace0, ", ")).parse_next(input)?;
    " ] ".parse_next(input)?;
    let with_ties =
        alt(("WithTies [ true ] ".value(true), empty.value(false))).parse_next(input)?;
    "Output [ ".parse_next(input)?;
    let outs: Vec<String> = separated(1.., alt((column_name, aliased_column)), (multispace0, ", "))
        .parse_next(input)?;
    let idx_to_table = &input.state.state.idx_to_table;
    if !validate_output(input_idx, &outs, idx_to_table) {
        return fail.parse_next(input);
    }
    let output_table = get_output(inputs, outs).parse_next(input)?;
    let state = &mut input.state.state;
    state.idx_to_table.insert(state.current_idx, output_table);
    " ]".parse_next(input)?;
    Ok(Operation::TopSort {
        input: input_idx,
        rows,
        order_by: obs,
        with_ties,
    })
}

fn validate_output(
    input_idx: usize,
    outs: &[String],
    idx_to_table: &HashMap<usize, Table>,
) -> bool {
    let prev_columns = idx_to_table[&input_idx]
        .columns()
        .iter()
        .map(|c| c.name())
        .collect::<HashSet<_>>();
    let is_subset_of_prev = outs.iter().all(|out| prev_columns.contains(out.as_str()));

    !has_duplicates(outs) && is_subset_of_prev
}
