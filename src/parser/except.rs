use super::{
    shared::{
        boolean, get_table_from_indexed_outputs, indexed_column, input_ids, null, number,
        predicate_wrapper, spaced_comparison_op, string, Stream,
    },
    utils::has_duplicates,
};
use crate::domain::{
    ColumnType, Comparable, Comparison, ExceptOperator, Operation, Predicate, Table,
};
use std::collections::{HashMap, HashSet};
use winnow::{
    ascii::multispace0,
    combinator::{alt, empty, fail, separated, separated_foldl1},
    error::ParserError,
    Parser,
};

pub(crate) fn except<'i, E: ParserError<Stream<'i>>>(
    with_type_checking: bool,
) -> impl Parser<Stream<'i>, Operation, E> {
    move |input: &mut Stream<'i>| {
        "Except ".parse_next(input)?;
        let inputs = input_ids.parse_next(input)?;
        if inputs.len() != 2 {
            return fail.parse_next(input);
        }
        let operator = alt((
            predicate_wrapper(predicate(with_type_checking, &inputs))
                .map(ExceptOperator::Predicate),
            except_columns(&inputs).map(ExceptOperator::ExceptColum),
        ))
        .parse_next(input)?;
        let is_distinct =
            alt(("Distinct [ true ] ".value(true), empty.value(false))).parse_next(input)?;
        "Output [ ".parse_next(input)?;
        let outs_with_index = alt((
            "1 AS One".map(|x: &'i str| vec![(usize::MAX, x.to_owned())]),
            separated(1.., indexed_column(&inputs), (multispace0, ", ")),
        ))
        .parse_next(input)?;
        let idx_to_table = &input.state.state.idx_to_table;
        if !validate_output(&inputs, &outs_with_index, idx_to_table) {
            return fail.parse_next(input);
        }
        let output_table = get_table_from_indexed_outputs(outs_with_index).parse_next(input)?;
        let state = &mut input.state.state;
        state.idx_to_table.insert(state.current_idx, output_table);
        " ]".parse_next(input)?;
        Ok(Operation::Except {
            inputs,
            operator,
            is_distinct,
        })
    }
}

fn except_columns<'i, 'j, E: ParserError<Stream<'i>>>(
    input_idxs: &'j [usize],
) -> impl Parser<Stream<'i>, String, E> + 'j {
    move |input: &mut Stream<'i>| {
        "ExceptColumns [ ".parse_next(input)?;
        let (_, column) = indexed_column(input_idxs).parse_next(input)?;
        " ] ".parse_next(input)?;
        Ok(column)
    }
}

fn predicate<'i, 'j, E: ParserError<Stream<'i>>>(
    with_type_checking: bool,
    input_idxs: &'j [usize],
) -> impl Parser<Stream<'i>, Predicate, E> + 'j {
    move |input: &mut Stream<'i>| {
        separated_foldl1(
            comparison(with_type_checking, input_idxs).map(|c| Predicate::Single { comparison: c }),
            alt((" AND ", " OR ")),
            |lhs, op, rhs| match op {
                " AND " => Predicate::And {
                    lhs: Box::new(lhs),
                    rhs: Box::new(rhs),
                },
                " OR " => Predicate::Or {
                    lhs: Box::new(lhs),
                    rhs: Box::new(rhs),
                },
                _ => panic!("Invalid operation on predicates: {}", op),
            },
        )
        .parse_next(input)
    }
}

fn comparison<'i, 'j, E: ParserError<Stream<'i>>>(
    with_type_checking: bool,
    input_idxs: &'j [usize],
) -> impl Parser<Stream<'i>, Comparison, E> + 'j {
    move |input: &mut Stream<'i>| {
        let (idx, column) = indexed_column(input_idxs).parse_next(input)?;
        let op = spaced_comparison_op.parse_next(input)?;
        if with_type_checking {
            let idx_to_table = &input.state.state.idx_to_table;
            let typ = idx_to_table[&idx].columns().iter().find_map(|c| {
                if c.name() == column {
                    Some(c.typ().clone())
                } else {
                    None
                }
            });
            if typ.is_none() {
                return fail.parse_next(input);
            }
            let typ = typ.unwrap();
            let rhs = type_comparable(input_idxs, &typ).parse_next(input)?;
            Ok(Comparison::from_string(
                &op,
                Comparable::Column(column),
                rhs,
            ))
        } else {
            let rhs = comparable(input_idxs).parse_next(input)?;
            Ok(Comparison::from_string(
                &op,
                Comparable::Column(column),
                rhs,
            ))
        }
    }
}

fn comparable<'i, 'j, E: ParserError<Stream<'i>>>(
    input_idxs: &'j [usize],
) -> impl Parser<Stream<'i>, Comparable, E> + 'j {
    move |input: &mut Stream<'i>| {
        alt((
            number,
            boolean,
            string,
            null,
            indexed_column(input_idxs).map(|(_, column)| Comparable::Column(column)),
        ))
        .parse_next(input)
    }
}

fn type_comparable<'i, 'j, E: ParserError<Stream<'i>>>(
    input_idxs: &'j [usize],
    lhs_type: &'j ColumnType,
) -> impl Parser<Stream<'i>, Comparable, E> + 'j {
    move |input: &mut Stream<'i>| {
        alt((null, column_in_index_of_type(lhs_type, input_idxs))).parse_next(input)
    }
}

fn column_in_index_of_type<'i, 'j, E: ParserError<Stream<'i>>>(
    typ: &'j ColumnType,
    input_idxs: &'j [usize],
) -> impl Parser<Stream<'i>, Comparable, E> + 'j {
    move |input: &mut Stream<'i>| {
        let (idx, column) = indexed_column(input_idxs).parse_next(input)?;
        let table = &input.state.state.idx_to_table[&idx];
        let is_column_in_table_of_type = table
            .columns()
            .iter()
            .any(|c| c.name() == column && c.typ() == typ);

        if is_column_in_table_of_type {
            Ok(Comparable::Column(column))
        } else {
            fail.parse_next(input)
        }
    }
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
