use super::{
    shared::{
        boolean, column_in_table, column_key, column_name, column_type, null, number,
        predicate_wrapper, spaced_comparison_op, string, table_name, Stream,
    },
    utils::has_duplicates,
};
use crate::domain::{
    Column, ColumnType, Comparable, Comparison, Operation, Predicate, SqlSchema, Table,
};
use winnow::{
    ascii::multispace0,
    combinator::{alt, empty, fail, opt, separated, separated_foldl1},
    error::ParserError,
    Parser,
};

pub(crate) fn scan<'i, E: ParserError<Stream<'i>>>(
    with_type_checking: bool,
) -> impl Parser<Stream<'i>, Operation, E> {
    move |input: &mut Stream<'i>| {
        "Scan Table [ ".parse_next(input)?;
        let table = table_name.parse_next(input)?;
        " ] ".parse_next(input)?;
        let predicate =
            opt(predicate_wrapper(predicate(with_type_checking, &table))).parse_next(input)?;
        let is_distinct =
            alt(("Distinct [ true ] ".value(true), empty.value(false))).parse_next(input)?;
        "Output [ ".parse_next(input)?;
        let outs_with_aliases = alt((
            "1 AS One".map(|x: &str| vec![(x.to_owned(), None)]),
            separated(1.., column_in_table(&table), (multispace0, ", ")),
        ))
        .parse_next(input)?;
        if has_duplicates(&outs_with_aliases) {
            return fail.parse_next(input);
        }
        let schema = &input.state.schema.as_ref().unwrap();
        let output_table = get_output_table(schema, &table, &outs_with_aliases);
        let state = &mut input.state.state;
        state.idx_to_table.insert(state.current_idx, output_table);
        " ]".parse_next(input)?;
        Ok(Operation::Scan {
            table,
            predicate,
            is_distinct,
        })
    }
}

fn predicate<'i, 't, E: ParserError<Stream<'i>>>(
    with_type_checking: bool,
    table: &'t str,
) -> impl Parser<Stream<'i>, Predicate, E> + 't {
    move |input: &mut Stream<'i>| {
        separated_foldl1(
            comparison(with_type_checking, table).map(|c| Predicate::Single { comparison: c }),
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

fn comparison<'i, 't, E: ParserError<Stream<'i>>>(
    with_type_checking: bool,
    table: &'t str,
) -> impl Parser<Stream<'i>, Comparison, E> + 't {
    move |input: &mut Stream<'i>| {
        let (column, _) = column_in_table(table).parse_next(input)?;
        let op = spaced_comparison_op.parse_next(input)?;
        if with_type_checking {
            let schema = &input.state.schema.as_ref().unwrap();
            let typ = column_type(schema, table, &column);
            if typ.is_none() {
                return fail.parse_next(input);
            }
            let rhs = type_comparable(typ.unwrap(), table).parse_next(input)?;
            Ok(Comparison::from_string(
                &op,
                Comparable::Column(column),
                rhs,
            ))
        } else {
            let rhs = comparable(table).parse_next(input)?;
            Ok(Comparison::from_string(
                &op,
                Comparable::Column(column),
                rhs,
            ))
        }
    }
}

fn comparable<'i, 't, E: ParserError<Stream<'i>>>(
    table: &'t str,
) -> impl Parser<Stream<'i>, Comparable, E> + 't {
    move |input: &mut Stream<'i>| {
        alt((
            number,
            boolean,
            string,
            null,
            column_in_table(table).map(|(column, _)| Comparable::Column(column)),
        ))
        .parse_next(input)
    }
}

fn type_comparable<'i, 't, E: ParserError<Stream<'i>>>(
    lhs_type: ColumnType,
    table: &'t str,
) -> impl Parser<Stream<'i>, Comparable, E> + 't {
    use ColumnType::*;
    move |input: &mut Stream<'i>| match lhs_type {
        Number => alt((number, null, column_in_table_of_type(Number, table))).parse_next(input),
        Boolean => alt((boolean, null, column_in_table_of_type(Boolean, table))).parse_next(input),
        Text => alt((string, null, column_in_table_of_type(Text, table))).parse_next(input),
        Time => alt((string, null, column_in_table_of_type(Time, table))).parse_next(input),
        Others => alt((
            number,
            boolean,
            string,
            null,
            column_in_table_of_type(Others, table),
        ))
        .parse_next(input),
    }
}

fn column_in_table_of_type<'i, 't, E: ParserError<Stream<'i>>>(
    typ: ColumnType,
    table: &'t str,
) -> impl Parser<Stream<'i>, Comparable, E> + 't {
    move |input: &mut Stream<'i>| {
        let column = column_name.parse_next(input)?;
        let schema = &input.state.schema.as_ref().unwrap();
        let ti = schema.table_names.iter().position(|t| t == table).unwrap();
        let is_column_in_table_of_type = schema.column_names.iter().enumerate().any(|(i, cn)| {
            *cn == column && schema.column_to_table[i] == ti && schema.column_types[i] == typ
        });

        if is_column_in_table_of_type {
            Ok(Comparable::Column(column))
        } else {
            fail.parse_next(input)
        }
    }
}

fn get_output_table(schema: &SqlSchema, table: &str, outs: &[(String, Option<String>)]) -> Table {
    Table::Named {
        name: table.to_owned(),
        columns: outs
            .iter()
            .map(|out| match out {
                (out, _) if out == "1 AS One" => Column::Dummy,
                (out, Some(alias)) => Column::Plain {
                    name: alias.to_owned(),
                    typ: column_type(schema, table, out).unwrap().clone(),
                    keys: column_key(schema, table, out),
                },
                (out, None) => Column::Plain {
                    name: out.to_owned(),
                    typ: column_type(schema, table, out).unwrap().clone(),
                    keys: column_key(schema, table, out),
                },
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::Operation;
    use crate::parser::shared::get_input;
    use winnow::{error::ContextError, stream::StreamIsPartial};

    #[test]
    fn test_scan_toy_example() {
        let mut input = get_input("Scan Table [ stadium ] Output [ Location ]");
        let _ = input.complete();
        let output = scan::<ContextError>(true).parse_next(&mut input).unwrap();
        assert_eq!(
            output,
            Operation::Scan {
                table: "stadium".to_owned(),
                predicate: None,
                is_distinct: false
            }
        )
    }

    #[test]
    fn test_scan_bigger_example() {
        let mut input = get_input(
            "Scan Table [ concert ] Predicate [ Year >= 2014 AND Year <= 2024 ] Distinct [ true ] Output [ Stadium_ID , Year ]",
        );
        let _ = input.complete();
        let output = scan::<ContextError>(true).parse_next(&mut input).unwrap();
        assert_eq!(
            output,
            Operation::Scan {
                table: "concert".to_owned(),
                predicate: Some(Predicate::And {
                    lhs: Box::new(Predicate::Single {
                        comparison: Comparison::GreaterThanOrEqual(
                            Comparable::Column("Year".to_owned()),
                            Comparable::Number(2014f64)
                        )
                    }),
                    rhs: Box::new(Predicate::Single {
                        comparison: Comparison::LessThanOrEqual(
                            Comparable::Column("Year".to_owned()),
                            Comparable::Number(2024f64)
                        )
                    })
                }),
                is_distinct: true
            }
        );
    }

    #[test]
    fn test_scan_fails_on_type_mismatch() {
        let mut input = get_input(
            "Scan Table [ concert ] Predicate [ Year >= '2014' ] Output [ Stadium_ID , Year ]",
        );
        let _ = input.complete();
        assert!(scan::<ContextError>(true).parse_next(&mut input).is_err());
    }

    #[test]
    fn test_scan_fails_on_duplicate_outputs() {
        let mut input = get_input("Scan Table [ concert ] Output [ Stadium_ID , Stadium_ID ]");
        let _ = input.complete();
        assert!(scan::<ContextError>(true).parse_next(&mut input).is_err());
    }
}
