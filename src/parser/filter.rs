use super::{
    shared::{
        aliased_column, boolean, column_in_index, column_name, get_output, input_ids, null, number,
        predicate_wrapper, spaced_comparison_op, string, ColumnParserType, Stream,
    },
    utils::has_duplicates,
};
use crate::domain::{ColumnType, Comparable, Comparison, Operation, Predicate, Table};
use std::collections::{HashMap, HashSet};
use winnow::{
    ascii::multispace0,
    combinator::{alt, empty, fail, opt, separated, separated_foldl1},
    error::ParserError,
    PResult, Parser,
};

pub(crate) fn filter<'i, E: ParserError<Stream<'i>>>(
    with_type_checking: bool,
) -> impl Parser<Stream<'i>, Operation, E> {
    move |input: &mut Stream<'i>| {
        "Filter ".parse_next(input)?;
        let inputs = input_ids.parse_next(input)?;
        if inputs.len() != 1 {
            return fail.parse_next(input);
        }
        let input_idx = inputs[0];
        let predicate =
            opt(predicate_wrapper(predicate(with_type_checking, input_idx))).parse_next(input)?;
        let is_distinct =
            alt(("Distinct [ true ] ".value(true), empty.value(false))).parse_next(input)?;
        "Output [ ".parse_next(input)?;

        let outs = alt((
            "1 AS One".map(|x: &str| vec![x.to_owned()]),
            separated(1.., alt((column_name, aliased_column)), (multispace0, ", ")),
        ))
        .parse_next(input)?;
        let idx_to_table = &input.state.state.idx_to_table;
        if !validate_output(input_idx, &outs, idx_to_table) {
            return fail.parse_next(input);
        }
        let output_table = get_output(inputs, outs).parse_next(input)?;
        let state = &mut input.state.state;
        state.idx_to_table.insert(state.current_idx, output_table);
        " ]".parse_next(input)?;
        Ok(Operation::Filter {
            input: input_idx,
            predicate,
            is_distinct,
        })
    }
}

fn predicate<'i, E: ParserError<Stream<'i>>>(
    with_type_checking: bool,
    input_idx: usize,
) -> impl Parser<Stream<'i>, Predicate, E> {
    move |input: &mut Stream<'i>| {
        separated_foldl1(
            comparison(with_type_checking, input_idx).map(|c| Predicate::Single { comparison: c }),
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

fn comparison<'i, E: ParserError<Stream<'i>>>(
    with_type_checking: bool,
    input_idx: usize,
) -> impl Parser<Stream<'i>, Comparison, E> {
    move |input: &mut Stream<'i>| {
        let column = alt((
            column_in_index(input_idx, ColumnParserType::Named),
            column_in_index(input_idx, ColumnParserType::Aliased),
        ))
        .parse_next(input)?;
        let op = spaced_comparison_op.parse_next(input)?;
        if with_type_checking {
            let state = &input.state.state;
            let typ = state.idx_to_table[&input_idx]
                .columns()
                .iter()
                .find_map(|c| {
                    if c.name() == column {
                        Some(c.typ())
                    } else {
                        None
                    }
                });
            if typ.is_none() {
                return fail.parse_next(input);
            }
            let rhs = type_comparable(typ.unwrap().clone(), input_idx).parse_next(input)?;
            Ok(Comparison::from_string(
                &op,
                Comparable::Column(column),
                rhs,
            ))
        } else {
            let rhs = comparable.parse_next(input)?;
            Ok(Comparison::from_string(
                &op,
                Comparable::Column(column),
                rhs,
            ))
        }
    }
}

fn comparable<'i, E: ParserError<Stream<'i>>>(input: &mut Stream<'i>) -> PResult<Comparable, E> {
    alt((
        number,
        boolean,
        string,
        null,
        column_name.map(Comparable::Column),
    ))
    .parse_next(input)
}

fn type_comparable<'i, E: ParserError<Stream<'i>>>(
    lhs_type: ColumnType,
    input_idx: usize,
) -> impl Parser<Stream<'i>, Comparable, E> {
    use ColumnType::*;
    move |input: &mut Stream<'i>| match lhs_type {
        Number => alt((number, null, column_in_table_of_type(Number, input_idx))).parse_next(input),
        Boolean => {
            alt((boolean, null, column_in_table_of_type(Boolean, input_idx))).parse_next(input)
        }
        Text => alt((string, null, column_in_table_of_type(Text, input_idx))).parse_next(input),
        Time => alt((string, null, column_in_table_of_type(Time, input_idx))).parse_next(input),
        Others => alt((
            number,
            boolean,
            string,
            null,
            column_in_table_of_type(Others, input_idx),
        ))
        .parse_next(input),
    }
}

fn column_in_table_of_type<'i, E: ParserError<Stream<'i>>>(
    typ: ColumnType,
    input_idx: usize,
) -> impl Parser<Stream<'i>, Comparable, E> {
    move |input: &mut Stream<'i>| {
        let column = column_name.parse_next(input)?;
        let state = &input.state.state;
        let table = &state.idx_to_table[&input_idx];
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
    input_idx: usize,
    outs: &[String],
    idx_to_table: &HashMap<usize, Table>,
) -> bool {
    let prev_columns = idx_to_table[&input_idx]
        .columns()
        .iter()
        .map(|c| c.name())
        .collect::<HashSet<_>>();
    let is_dummy = outs == ["1 AS One".to_owned()];
    let is_subset_of_prev = outs.iter().all(|out| prev_columns.contains(out.as_str()));

    !has_duplicates(outs) && (is_subset_of_prev || is_dummy)
}
