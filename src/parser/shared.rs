use super::utils::*;
use crate::domain::*;
use winnow::{
    ascii::{alphanumeric1, dec_uint, float, multispace0, Caseless},
    combinator::{alt, delimited, fail, opt, separated},
    error::ParserError,
    token::take_while,
    PResult, Parser, Partial, Stateful,
};

#[cfg(test)]
use crate::schemas::concert_singer;

pub(crate) type Stream<'i> = Stateful<Partial<&'i str>, QplEnvironment>;

pub(crate) fn choice<'i, E: ParserError<Stream<'i>>>(
    choices: Vec<String>,
) -> impl Parser<Stream<'i>, String, E> {
    move |input: &mut Stream<'i>| {
        for choice in choices.iter() {
            if (opt(Caseless(choice.as_str())).parse_next(input)?).is_some() {
                return Ok(choice.to_owned());
            }
        }
        fail.parse_next(input)
    }
}

pub(crate) fn table_name<'i, E: ParserError<Stream<'i>>>(
    input: &mut Stream<'i>,
) -> PResult<String, E> {
    let schema = &input.state.schema.as_ref().unwrap();
    let mut table_names = schema.table_names.clone();

    table_names.sort_unstable_by(|a, b| cmp_length_desc(a, b));

    choice(table_names).parse_next(input)
}

pub(crate) fn column_name<'i, E: ParserError<Stream<'i>>>(
    input: &mut Stream<'i>,
) -> PResult<String, E> {
    let schema = &input.state.schema.as_ref().unwrap();
    let mut column_names = schema.column_names.clone();

    column_names.sort_unstable_by(|a, b| cmp_length_desc(a, b));

    choice(column_names).parse_next(input)
}

pub(crate) fn aliased_column<'i, E: ParserError<Stream<'i>>>(
    input: &mut Stream<'i>,
) -> PResult<String, E> {
    let state = &input.state.state;
    let previous_aliases = state
        .idx_to_table
        .values()
        .flat_map(|table| {
            table
                .columns()
                .iter()
                .map(|c| c.name().to_owned())
                .collect::<Vec<_>>()
        })
        .filter(|x| starts_with_agg(x))
        .collect::<Vec<_>>();

    choice(previous_aliases).parse_next(input)
}

pub(crate) fn column_in_table<'i, 't, E: ParserError<Stream<'i>>>(
    table: &'t str,
) -> impl Parser<Stream<'i>, (String, Option<String>), E> + 't {
    move |input: &mut Stream<'i>| {
        let column = column_name.parse_next(input)?;
        let alias = opt((" AS ", alphanumeric1))
            .map(|alias_opt| alias_opt.map(|(_, alias)| alias))
            .parse_next(input)?;
        let schema = &input.state.schema.as_ref().unwrap();
        let t = schema.table_names.iter().position(|t| t == table).unwrap();
        let is_column_in_table = schema.column_names.iter().enumerate().any(|(i, cn)| {
            cn.to_lowercase() == column.to_lowercase() && schema.column_to_table[i] == t
        });

        if is_column_in_table {
            Ok((column.to_owned(), alias.map(|s| s.to_owned())))
        } else {
            fail.parse_next(input)
        }
    }
}

pub(crate) enum ColumnParserType {
    Named,
    Aliased,
}

pub(crate) fn column_in_index<'i, E: ParserError<Stream<'i>>>(
    idx: usize,
    column_parser_type: ColumnParserType,
) -> impl Parser<Stream<'i>, String, E> {
    move |input: &mut Stream<'i>| {
        let column = match column_parser_type {
            ColumnParserType::Named => column_name.parse_next(input)?,
            ColumnParserType::Aliased => aliased_column.parse_next(input)?,
        };
        let state = &input.state.state;
        let is_column_in_table = state.idx_to_table.get(&idx).map_or_else(
            || false,
            |table| {
                table.columns().iter().any(|c| match c {
                    Column::Dummy => false,
                    Column::Plain { name, .. } => column == *name,
                    Column::Aliased { name, .. } => column == *name,
                })
            },
        );
        if is_column_in_table {
            Ok(column)
        } else {
            fail.parse_next(input)
        }
    }
}

pub(crate) fn indexed_column<'i, 'j, E: ParserError<Stream<'i>>>(
    inputs: &'j [usize],
) -> impl Parser<Stream<'i>, (usize, String), E> + 'j {
    move |input: &mut Stream<'i>| {
        "#".parse_next(input)?;
        let idx = dec_uint.parse_next(input)?;
        if !inputs.contains(&idx) {
            return fail.parse_next(input);
        }
        ".".parse_next(input)?;
        let column = alt((
            column_in_index(idx, ColumnParserType::Named),
            column_in_index(idx, ColumnParserType::Aliased),
        ))
        .parse_next(input)?;
        Ok((idx, column))
    }
}

pub(crate) fn comparison_op<'i, E: ParserError<Stream<'i>>>(
    input: &mut Stream<'i>,
) -> PResult<String, E> {
    let op = alt((
        "<>",
        "<=",
        ">=",
        Caseless("is not").value("IS NOT"),
        Caseless("is").value("IS"),
        Caseless("like").value("LIKE"),
        Caseless("not like").value("NOT LIKE"),
        "<",
        ">",
        "=",
    ))
    .parse_next(input)?;
    Ok(op.to_owned())
}

pub(crate) fn number<'i, E: ParserError<Stream<'i>>>(
    input: &mut Stream<'i>,
) -> PResult<Comparable, E> {
    float.parse_next(input).map(Comparable::Number)
}

pub(crate) fn string<'i, E: ParserError<Stream<'i>>>(
    input: &mut Stream<'i>,
) -> PResult<Comparable, E> {
    "'".parse_next(input)?;
    let string = take_while(0.., |c| c != '\'').parse_next(input)?;
    "'".parse_next(input)?;
    Ok(Comparable::Str(string.to_owned()))
}

pub(crate) fn boolean<'i, E: ParserError<Stream<'i>>>(
    input: &mut Stream<'i>,
) -> PResult<Comparable, E> {
    alt(("0".value(false), "1".value(true)))
        .map(Comparable::Boolean)
        .parse_next(input)
}

pub(crate) fn null<'i, E: ParserError<Stream<'i>>>(
    input: &mut Stream<'i>,
) -> PResult<Comparable, E> {
    "NULL".value(Comparable::Null).parse_next(input)
}

pub(crate) fn input_ids<'i, E: ParserError<Stream<'i>>>(
    input: &mut Stream<'i>,
) -> PResult<Vec<usize>, E> {
    "[ ".parse_next(input)?;
    let single = ("#", dec_uint).map(|(_, id): (&str, usize)| id);
    let ids: Vec<usize> = separated(1..=2, single, (multispace0, ", ")).parse_next(input)?;
    let state = &input.state.state;
    if !ids.iter().all(|id| state.seen.contains(id)) {
        return fail.parse_next(input);
    }
    " ] ".parse_next(input)?;
    Ok(ids)
}

pub(crate) fn predicate_wrapper<'i, E: ParserError<Stream<'i>>>(
    mut inner: impl Parser<Stream<'i>, Predicate, E>,
) -> impl Parser<Stream<'i>, Predicate, E> {
    move |input: &mut Stream<'i>| {
        "Predicate [ ".parse_next(input)?;
        let p = inner.parse_next(input)?;
        " ] ".parse_next(input)?;
        Ok(p)
    }
}

pub(crate) fn column_type(schema: &SqlSchema, table: &str, column: &str) -> Option<ColumnType> {
    let t = schema.table_names.iter().position(|t| t == table)?;
    let c = schema
        .column_names
        .iter()
        .enumerate()
        .find(|(i, cn)| {
            cn.to_lowercase() == column.to_lowercase() && schema.column_to_table[*i] == t
        })
        .map(|(i, _)| i)?;
    Some(schema.column_types[c].clone())
}

pub(crate) fn column_key(schema: &SqlSchema, table: &str, column: &str) -> Vec<KeyType> {
    let t = schema.table_names.iter().position(|t| t == table).unwrap();
    let c = schema
        .column_names
        .iter()
        .enumerate()
        .find(|(i, cn)| {
            cn.to_lowercase() == column.to_lowercase() && schema.column_to_table[*i] == t
        })
        .map(|(i, _)| i);

    let (fks, pks): (Vec<usize>, Vec<usize>) =
        schema.foreign_keys.iter().map(|(a, b)| (*a, *b)).unzip();

    match c {
        Some(i) => {
            let pk = if schema.primary_keys.contains(&i) || pks.contains(&i) {
                vec![KeyType::PrimaryKey {
                    table: table.to_owned(),
                }]
            } else {
                vec![]
            };
            let mut fk = if fks.contains(&i) {
                let mut fks = pks
                    .iter()
                    .map(|pk| KeyType::ForeignKey {
                        table: schema.table_names[schema.column_to_table[*pk]].clone(),
                    })
                    .collect::<Vec<_>>();
                fks.sort();
                fks.dedup();
                fks
            } else {
                vec![]
            };

            let mut result = pk;
            result.append(&mut fk);
            result
        }
        None => vec![],
    }
}

pub(crate) fn get_table_from_indexed_outputs<'i, E: ParserError<Stream<'i>>>(
    outs: Vec<(usize, String)>,
) -> impl Parser<Stream<'i>, Table, E> {
    move |input: &mut Stream<'i>| {
        let state = &input.state.state;
        let columns = outs
            .iter()
            .map(|out| match out {
                (_, out) if out == "1 AS One" => Some(Column::Dummy),
                (_, out) if out == "countstar AS Count_Star" => Some(Column::Aliased {
                    name: "Count_Star".to_owned(),
                    typ: ColumnType::Number,
                    keys: vec![],
                }),
                (_, out) if starts_with_agg(out) => Some(Column::Aliased {
                    name: out.to_owned(),
                    typ: ColumnType::Number,
                    keys: vec![],
                }),
                (idx, out) => state.idx_to_table[&idx]
                    .columns()
                    .iter()
                    .find(|c| c.name() == out)
                    .cloned(),
            })
            .collect::<Option<Vec<_>>>();

        match columns {
            Some(cs) => Ok(Table::Indexed {
                idx: state.current_idx,
                columns: cs,
            }),
            None => fail.parse_next(input),
        }
    }
}

pub(crate) fn get_output<'i, E: ParserError<Stream<'i>>>(
    inputs: Vec<usize>,
    outs: Vec<String>,
) -> impl Parser<Stream<'i>, Table, E> {
    move |input: &mut Stream<'i>| {
        let current_idx = input.state.state.current_idx;
        let prev = inputs
            .iter()
            .map(|i| &input.state.state.idx_to_table[i])
            .collect::<Vec<_>>();
        let columns = outs
            .iter()
            .map(|out| match out {
                out if out == "1 AS One" => Some(Column::Dummy),
                out if out == "countstar AS Count_Star" => Some(Column::Aliased {
                    name: "Count_Star".to_owned(),
                    typ: ColumnType::Number,
                    keys: vec![],
                }),
                out if starts_with_agg(out.as_str()) => Some(Column::Aliased {
                    name: out.to_owned(),
                    typ: ColumnType::Number,
                    keys: vec![],
                }),
                out => prev
                    .iter()
                    .fold(None, |res, table| {
                        res.or(table.columns().iter().find(|c| c.name() == out))
                    })
                    .cloned(),
            })
            .collect::<Option<Vec<_>>>();

        match columns {
            Some(cs) => Ok(Table::Indexed {
                idx: current_idx,
                columns: cs,
            }),
            None => fail.parse_next(input),
        }
    }
}

pub(crate) fn order_by<'i, E: ParserError<Stream<'i>>>(
    input_idx: usize,
) -> impl Parser<Stream<'i>, String, E> {
    move |input: &mut Stream<'i>| {
        let by = alt((aliased_column, column_name)).parse_next(input)?;
        let state = &input.state.state;
        let is_valid_column = state.idx_to_table[&input_idx]
            .columns()
            .iter()
            .any(|c| c.name() == by);
        if !is_valid_column {
            return fail.parse_next(input);
        }
        multispace0.parse_next(input)?;
        let dir = alt(("ASC", "DESC")).parse_next(input)?;
        Ok(format!("{by} {dir}"))
    }
}

pub(crate) fn spaced_comparison_op<'i, E: ParserError<Stream<'i>>>(
    input: &mut Stream<'i>,
) -> PResult<String, E> {
    delimited(multispace0, comparison_op, multispace0).parse_next(input)
}

#[cfg(test)]
pub(crate) fn get_input(input: &str) -> Stream<'_> {
    let schema = Some(concert_singer());
    let state = QplState::default();
    let env = QplEnvironment { state, schema };
    Stream {
        input: Partial::new(input),
        state: env,
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use winnow::{
        error::{ContextError, ErrMode},
        stream::StreamIsPartial,
    };

    #[test]
    fn test_table_name_complete_table_exists() {
        let mut input = get_input("singer");
        let _ = input.complete();
        let result = table_name::<ContextError>.parse_next(&mut input).unwrap();
        assert_eq!(result, "singer")
    }

    #[test]
    fn test_table_name_partial_parse() {
        let mut input = get_input("sing");
        match table_name::<ContextError>.parse_next(&mut input) {
            Err(ErrMode::Incomplete(_)) => {}
            _ => panic!("\"sing\" should be a partial parse of the table \"singer\""),
        }
    }

    #[test]
    fn test_table_name_table_does_not_exist() {
        let mut input = get_input("foobar");
        assert!(table_name::<ContextError>.parse_next(&mut input).is_err());
    }

    #[test]
    fn test_input_ids_one_id() {
        let mut input = get_input("[ #1 ] ");
        input.state.state.seen.insert(1);
        let output = input_ids::<ContextError>.parse_next(&mut input).unwrap();
        assert_eq!(output, vec![1]);
    }

    #[test]
    fn test_input_ids_two_ids() {
        let mut input = get_input("[ #1, #2 ] ");
        input.state.state.seen.insert(1);
        input.state.state.seen.insert(2);
        let output = input_ids::<ContextError>.parse_next(&mut input).unwrap();
        assert_eq!(output, vec![1, 2]);
    }

    #[test]
    fn test_input_ids_fails_if_ids_not_seen() {
        let mut input = get_input("[ #1, #2 ] ");
        assert!(input_ids::<ContextError>.parse_next(&mut input).is_err());
    }

    #[test]
    fn test_column_name_returns_original_column_name() {
        let mut input = get_input("stadium_id");
        let output = column_name::<ContextError>.parse_next(&mut input).unwrap();
        assert_eq!(output, "Stadium_ID");
    }

    #[test]
    fn test_column_in_table_returns_existing_column_without_alias() {
        let mut input = get_input("Stadium_ID");
        let _ = input.complete();
        let (column, alias) = column_in_table::<ContextError>("stadium")
            .parse_next(&mut input)
            .unwrap();
        assert_eq!(column, "Stadium_ID");
        assert!(alias.is_none());
    }

    #[test]
    fn test_column_in_table_returns_existing_column_with_alias() {
        let mut input = get_input("Stadium_ID AS sid");
        let _ = input.complete();
        let (column, alias) = column_in_table::<ContextError>("stadium")
            .parse_next(&mut input)
            .unwrap();
        assert_eq!(column, "Stadium_ID");
        assert_eq!(alias, Some("sid".to_owned()));
    }
}
