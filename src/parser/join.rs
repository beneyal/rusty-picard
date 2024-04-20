use super::{
    shared::{
        boolean, get_table_from_indexed_outputs, indexed_column, input_ids, null, number,
        predicate_wrapper, spaced_comparison_op, string, Stream,
    },
    utils::has_duplicates,
};
use crate::domain::{
    Column, ColumnType, Comparable, Comparison, KeyType, Operation, Predicate, Table,
};
use std::collections::{HashMap, HashSet};
use winnow::{
    ascii::multispace0,
    combinator::{alt, empty, fail, opt, separated, separated_foldl1, todo},
    error::ParserError,
    Parser,
};

pub(crate) fn join<'i, E: ParserError<Stream<'i>>>(
    with_type_checking: bool,
) -> impl Parser<Stream<'i>, Operation, E> {
    move |input: &mut Stream<'i>| {
        "Join ".parse_next(input)?;
        let inputs = input_ids.parse_next(input)?;
        if inputs.len() != 2 {
            return fail.parse_next(input);
        }
        let predicate =
            opt(predicate_wrapper(predicate(with_type_checking, &inputs))).parse_next(input)?;
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
        Ok(Operation::Join {
            inputs,
            predicate,
            is_distinct,
        })
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
            let lhs_data = idx_to_table[&idx].columns().iter().find_map(|c| {
                if c.name() == column {
                    Some((
                        c.typ().clone(),
                        c.keys().to_vec(),
                        matches!(c, Column::Aliased { .. }),
                    ))
                } else {
                    None
                }
            });
            if lhs_data.is_none() {
                return fail.parse_next(input);
            }
            let (typ, keys, is_aliased) = lhs_data.unwrap();
            let rhs = if op == "=" && !is_aliased {
                comparable_key(input_idxs, &typ, &keys).parse_next(input)
            } else {
                type_comparable(input_idxs, &typ).parse_next(input)
            }?;
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

fn comparable_key<'i, 'j, E: ParserError<Stream<'i>>>(
    input_idxs: &'j [usize],
    lhs_type: &'j ColumnType,
    lhs_keys: &'j [KeyType],
) -> impl Parser<Stream<'i>, Comparable, E> + 'j {
    let p1 = move |input: &mut Stream<'i>| {
        for key in lhs_keys {
            let choice = match key {
                KeyType::PrimaryKey { table } => opt(comparable_key_and_type(
                    input_idxs,
                    lhs_type,
                    table,
                    is_foreign_key_of,
                ))
                .parse_next(input),
                KeyType::ForeignKey { table } => opt(alt((
                    comparable_key_and_type(input_idxs, lhs_type, table, is_foreign_key_of),
                    comparable_key_and_type(input_idxs, lhs_type, table, is_primary_key_of),
                )))
                .parse_next(input),
            }?;
            if let Some(comparable) = choice {
                return Ok(comparable);
            }
        }
        fail.parse_next(input)
    };

    let p2 = move |input: &mut Stream<'i>| {
        let (idx, column) = indexed_column(input_idxs).parse_next(input)?;
        let t = &input.state.state.idx_to_table[&idx];
        let is_valid = t.columns().iter().any(|c| match c {
            Column::Aliased { name, typ, .. } => *name == column && typ == lhs_type,
            _ => todo!(),
        });
        if is_valid {
            Ok(Comparable::Column(column))
        } else {
            fail.parse_next(input)
        }
    };

    move |input: &mut Stream<'i>| alt((p1, p2)).parse_next(input)
}

fn comparable_key_and_type<'i, 'j, E: ParserError<Stream<'i>>>(
    input_idxs: &'j [usize],
    lhs_type: &'j ColumnType,
    lhs_table: &'j str,
    decider: impl Fn(KeyType, &'j str) -> bool + 'j,
) -> impl Parser<Stream<'i>, Comparable, E> + 'j {
    move |input: &mut Stream<'i>| {
        let (idx, column) = indexed_column(input_idxs).parse_next(input)?;
        let t = &input.state.state.idx_to_table[&idx];
        let is_column_in_table_of_type_and_key = t.columns().iter().any(|c| {
            c.name() == column
                && c.typ() == lhs_type
                && c.keys().iter().any(|key| decider(key.clone(), lhs_table))
        });
        if is_column_in_table_of_type_and_key {
            Ok(Comparable::Column(column))
        } else {
            fail.parse_next(input)
        }
    }
}

fn is_primary_key_of(key: KeyType, table: &str) -> bool {
    match key {
        KeyType::PrimaryKey { table: t } => t == table,
        KeyType::ForeignKey { .. } => false,
    }
}

fn is_foreign_key_of(key: KeyType, table: &str) -> bool {
    match key {
        KeyType::PrimaryKey { .. } => false,
        KeyType::ForeignKey { table: t } => t == table,
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
    use ColumnType::*;
    move |input: &mut Stream<'i>| match lhs_type {
        Number => {
            alt((number, null, column_in_index_of_type(Number, input_idxs))).parse_next(input)
        }
        Boolean => {
            alt((boolean, null, column_in_index_of_type(Boolean, input_idxs))).parse_next(input)
        }
        Text => alt((string, null, column_in_index_of_type(Text, input_idxs))).parse_next(input),
        Time => alt((string, null, column_in_index_of_type(Time, input_idxs))).parse_next(input),
        Others => alt((
            number,
            boolean,
            string,
            null,
            column_in_index_of_type(Others, input_idxs),
        ))
        .parse_next(input),
    }
}

fn column_in_index_of_type<'i, 'j, E: ParserError<Stream<'i>>>(
    typ: ColumnType,
    input_idxs: &'j [usize],
) -> impl Parser<Stream<'i>, Comparable, E> + 'j {
    move |input: &mut Stream<'i>| {
        let (idx, column) = indexed_column(input_idxs).parse_next(input)?;
        let table = &input.state.state.idx_to_table[&idx];
        let is_column_in_table_of_type = table
            .columns()
            .iter()
            .any(|c| c.name() == column && *c.typ() == typ);

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
