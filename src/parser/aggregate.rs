use super::{
    shared::{column_in_index, column_name, get_output, input_ids, ColumnParserType, Stream},
    utils::{has_duplicates, starts_with_agg},
};
use crate::domain::{Agg, Operation, Table};
use std::collections::{HashMap, HashSet};
use winnow::{
    ascii::{multispace0, Caseless},
    combinator::{alt, empty, fail, opt, separated},
    error::ParserError,
    PResult, Parser,
};

pub(crate) fn aggregate<'i, E: ParserError<Stream<'i>>>(
    input: &mut Stream<'i>,
) -> PResult<Operation, E> {
    "Aggregate ".parse_next(input)?;
    let inputs = input_ids.parse_next(input)?;
    if inputs.len() != 1 {
        return fail.parse_next(input);
    }
    let input_idx = inputs[0];
    let gbs = opt(group_by(input_idx)).parse_next(input)?;
    "Output [ ".parse_next(input)?;
    let outs = outputs(input_idx).parse_next(input)?;
    let idx_to_table = &input.state.state.idx_to_table;
    if !validate_output(input_idx, &outs, idx_to_table) {
        return fail.parse_next(input);
    }
    let output_table = get_output(inputs, outs).parse_next(input)?;
    let state = &mut input.state.state;
    state.idx_to_table.insert(state.current_idx, output_table);
    " ]".parse_next(input)?;
    Ok(Operation::Aggregate {
        input: input_idx,
        group_by: gbs.unwrap_or(vec![]),
    })
}

fn group_by<'i, E: ParserError<Stream<'i>>>(
    input_idx: usize,
) -> impl Parser<Stream<'i>, Vec<String>, E> {
    move |input: &mut Stream<'i>| {
        "GroupBy [ ".parse_next(input)?;
        let columns = separated(
            1..,
            column_in_index(input_idx, ColumnParserType::Named),
            (multispace0, ", "),
        )
        .parse_next(input)?;
        " ] ".parse_next(input)?;
        Ok(columns)
    }
}

fn outputs<'i, E: ParserError<Stream<'i>>>(
    input_idx: usize,
) -> impl Parser<Stream<'i>, Vec<String>, E> {
    move |input: &mut Stream<'i>| {
        separated(
            1..,
            alt((
                "countstar AS Count_Star".map(|s: &'i str| s.to_owned()),
                aliased_aggregate(input_idx),
                column_name,
            )),
            (multispace0, ", "),
        )
        .parse_next(input)
    }
}

fn aliased_aggregate<'i, E: ParserError<Stream<'i>>>(
    input_idx: usize,
) -> impl Parser<Stream<'i>, String, E> {
    move |input: &mut Stream<'i>| {
        let aggregate = agg.parse_next(input)?;
        "(".parse_next(input)?;
        let is_distinct = alt(("DISTINCT ".value(true), empty.value(false))).parse_next(input)?;
        let column = column_in_index(input_idx, ColumnParserType::Named).parse_next(input)?;
        ") AS ".parse_next(input)?;
        let prefix = format!("{}_", aggregate).as_str().parse_next(input)?;
        let dist = if is_distinct {
            "Dist_".parse_next(input)
        } else {
            empty.value("").parse_next(input)
        }?;
        let alias = Caseless(column.as_str()).parse_next(input)?;
        Ok(format!("{}{}{}", prefix, dist, alias))
    }
}

fn agg<'i, E: ParserError<Stream<'i>>>(input: &mut Stream<'i>) -> PResult<Agg, E> {
    for agg in Agg::values() {
        if let Some(a) = opt(agg.to_string().to_uppercase().value(agg)).parse_next(input)? {
            return Ok(a);
        }
    }
    fail.parse_next(input)
}

fn validate_output(
    input_idx: usize,
    outs: &[String],
    idx_to_table: &HashMap<usize, Table>,
) -> bool {
    let without_aliases = outs
        .iter()
        .filter(|out| !starts_with_agg(out))
        .collect::<Vec<_>>();
    let prev_columns = idx_to_table[&input_idx]
        .columns()
        .iter()
        .map(|c| c.name())
        .collect::<HashSet<_>>();
    let is_subset_of_prev = without_aliases
        .iter()
        .all(|out| prev_columns.contains(out.as_str()));

    is_subset_of_prev && !has_duplicates(outs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{Column, ColumnType, QplState};
    use crate::parser::shared::get_input;
    use winnow::{error::ContextError, stream::StreamIsPartial};

    #[test]
    fn test_aggregate_count_star() {
        let mut input = get_input("Aggregate [ #1 ] Output [ countstar AS Count_Star ]");
        input.state.state = QplState {
            current_idx: 1,
            seen: HashSet::from([1]),
            idx_to_table: HashMap::from([(
                1,
                Table::Named {
                    name: "concert".to_owned(),
                    columns: vec![Column::Dummy],
                },
            )]),
        };
        let _ = input.complete();
        let output = aggregate::<ContextError>.parse_next(&mut input).unwrap();
        assert_eq!(
            output,
            Operation::Aggregate {
                input: 1,
                group_by: vec![]
            }
        )
    }

    #[test]
    fn test_aggregate_with_group_by() {
        let mut input =
            get_input("Aggregate [ #1 ] GroupBy [ Theme ] Output [ countstar AS Count_Star ]");
        input.state.state = QplState {
            current_idx: 1,
            seen: HashSet::from([1]),
            idx_to_table: HashMap::from([(
                1,
                Table::Named {
                    name: "concert".to_owned(),
                    columns: vec![Column::Plain {
                        name: "Theme".to_owned(),
                        typ: ColumnType::Text,
                        keys: vec![],
                    }],
                },
            )]),
        };
        let _ = input.complete();
        let output = aggregate::<ContextError>.parse_next(&mut input).unwrap();
        assert_eq!(
            output,
            Operation::Aggregate {
                input: 1,
                group_by: vec!["Theme".to_owned()]
            }
        )
    }

    #[test]
    fn test_aggregate_with_max() {
        let mut input = get_input("Aggregate [ #1 ] Output [ MAX(Age) AS Max_Age ]");
        input.state.state = QplState {
            current_idx: 1,
            seen: HashSet::from([1]),
            idx_to_table: HashMap::from([(
                1,
                Table::Named {
                    name: "singer".to_owned(),
                    columns: vec![Column::Plain {
                        name: "Age".to_owned(),
                        typ: ColumnType::Number,
                        keys: vec![],
                    }],
                },
            )]),
        };
        let _ = input.complete();
        let output = aggregate::<ContextError>.parse_next(&mut input).unwrap();
        assert_eq!(
            output,
            Operation::Aggregate {
                input: 1,
                group_by: vec![]
            }
        )
    }

    #[test]
    fn test_aggregate_with_count_distinct() {
        let mut input =
            get_input("Aggregate [ #1 ] Output [ COUNT(DISTINCT Age) AS Count_Dist_Age ]");
        input.state.state = QplState {
            current_idx: 1,
            seen: HashSet::from([1]),
            idx_to_table: HashMap::from([(
                1,
                Table::Named {
                    name: "singer".to_owned(),
                    columns: vec![Column::Plain {
                        name: "Age".to_owned(),
                        typ: ColumnType::Number,
                        keys: vec![],
                    }],
                },
            )]),
        };
        let _ = input.complete();
        let output = aggregate::<ContextError>.parse_next(&mut input).unwrap();
        assert_eq!(
            output,
            Operation::Aggregate {
                input: 1,
                group_by: vec![]
            }
        )
    }

    #[test]
    fn test_aggregate_fails_if_alias_is_wrong() {
        let mut input = get_input("Aggregate [ #1 ] Output [ MAX(Age) AS Foo ]");
        input.state.state = QplState {
            current_idx: 1,
            seen: HashSet::from([1]),
            idx_to_table: HashMap::from([(
                1,
                Table::Named {
                    name: "singer".to_owned(),
                    columns: vec![Column::Plain {
                        name: "Age".to_owned(),
                        typ: ColumnType::Number,
                        keys: vec![],
                    }],
                },
            )]),
        };
        let _ = input.complete();
        assert!(aggregate::<ContextError>.parse_next(&mut input).is_err());
    }
}
