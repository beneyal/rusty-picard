use super::{qpl, shared::Stream};
use crate::domain::{Qpl, SqlSchema};
use std::collections::HashMap;
use winnow::{
    ascii::{multispace0, Caseless},
    combinator::{alt, fail, opt, repeat},
    error::ParserError,
    PResult, Parser,
};

pub(crate) fn prefixed_qpl<'i, 'j, E: ParserError<Stream<'i>>>(
    schemas: &'j HashMap<String, SqlSchema>,
    with_type_checking: bool,
) -> impl Parser<Stream<'i>, Qpl, E> + 'j {
    move |input: &mut Stream<'i>| {
        multispace0.parse_next(input)?;
        repeat(0.., special_token).parse_next(input)?;
        multispace0.parse_next(input)?;
        let schema = schema(schemas).parse_next(input)?;
        input.state.schema = Some(schema.clone());
        (multispace0, "|", multispace0).parse_next(input)?;
        qpl(with_type_checking).parse_next(input)
    }
}

fn schema<'i, 'j, E: ParserError<Stream<'i>>>(
    schemas: &'j HashMap<String, SqlSchema>,
) -> impl Parser<Stream<'i>, &'j SqlSchema, E> + 'j {
    move |input: &mut Stream<'i>| {
        let mut schemas = Vec::from_iter(schemas.iter());
        schemas.sort_unstable_by_key(|(db_id, _)| db_id.chars().count());
        schemas.reverse();
        for (db_id, schema) in schemas {
            if opt(Caseless(db_id.as_str())).parse_next(input)?.is_some() {
                return Ok(schema);
            }
        }
        fail.parse_next(input)
    }
}

fn special_token<'i, E: ParserError<Stream<'i>>>(input: &mut Stream<'i>) -> PResult<(), E> {
    ("<", alt(("pad", "s", "/s")), ">").void().parse_next(input)
}
