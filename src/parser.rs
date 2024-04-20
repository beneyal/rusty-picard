mod aggregate;
mod except;
mod filter;
mod intersect;
mod join;
mod scan;
pub(crate) mod shared;
mod sort;
mod top;
mod top_sort;
mod union;
mod utils;

use self::shared::Stream;
use crate::domain::{Line, Qpl};
use aggregate::aggregate;
use except::except;
use filter::filter;
use intersect::intersect;
use join::join;
use scan::scan;
use sort::sort;
use top::top;
use top_sort::top_sort;
use union::union;
use winnow::{
    combinator::{alt, eof, separated},
    error::ParserError,
    Parser,
};

pub(crate) fn qpl<'i, E: ParserError<Stream<'i>>>(
    with_type_checking: bool,
) -> impl Parser<Stream<'i>, Qpl, E> {
    move |input: &mut Stream<'i>| {
        (separated(1.., qpl_line(with_type_checking), " ; "), eof)
            .map(|(qpl, _)| qpl)
            .parse_next(input)
    }
}

fn qpl_line<'i, E: ParserError<Stream<'i>>>(
    with_type_checking: bool,
) -> impl Parser<Stream<'i>, Line, E> {
    move |input: &mut Stream<'i>| {
        let current_idx = input.state.state.current_idx + 1;
        format!("#{} = ", current_idx).as_str().parse_next(input)?;
        input.state.state.current_idx += 1;
        let operation = alt((
            scan(with_type_checking),
            aggregate,
            filter(with_type_checking),
            top,
            sort,
            top_sort,
            join(with_type_checking),
            intersect(with_type_checking),
            except(with_type_checking),
            union,
        ))
        .parse_next(input)?;
        input.state.state.seen.insert(current_idx);
        Ok(Line {
            idx: current_idx,
            operation,
        })
    }
}

#[cfg(test)]
mod tests {
    use self::shared::get_input;
    use super::*;
    use crate::domain::Operation;
    use winnow::{
        error::{ContextError, ErrMode},
        stream::StreamIsPartial,
    };

    const POSITIVES: [&str; 8] = [
      "#1 = Scan Table [ stadium ] Output [ Stadium_ID , Capacity , Name ] ; #2 = Scan Table [ concert ] Predicate [ Year >= 2014 ] Output [ Stadium_ID , Year ] ; #3 = Aggregate [ #2 ] GroupBy [ Stadium_ID ] Output [ Stadium_ID , countstar AS Count_Star ] ; #4 = Join [ #1 , #3 ] Predicate [ #3.Stadium_ID = #1.Stadium_ID ] Output [ #1.Name , #3.Count_Star , #1.Capacity ] ; #5 = TopSort [ #4 ] Rows [ 1 ] OrderBy [ Count_Star DESC ] Output [ Capacity , Count_Star , Name ]",
      "#1 = Scan Table [ stadium ] Output [ Stadium_ID , Name ] ; #2 = Scan Table [ concert ] Output [ Stadium_ID ] ; #3 = Except [ #1 , #2 ] Predicate [ #2.Stadium_ID IS NULL OR #1.Stadium_ID = #2.Stadium_ID ] Output [ #1.Name ]",
      "#1 = Scan Table [ singer ] Predicate [ Country = 'france' ] Output [ Age , Country ] ; #2 = Aggregate [ #1 ] Output [ AVG(Age) AS Avg_Age , MAX(Age) AS Max_Age , MIN(Age) AS Min_Age ]",
      "#1 = Scan Table [ singer ] Output [ Singer_ID , Name ] ; #2 = Scan Table [ singer_in_concert ] Output [ Singer_ID ] ; #3 = Aggregate [ #2 ] GroupBy [ Singer_ID ] Output [ Singer_ID , countstar AS Count_Star ] ; #4 = Join [ #1 , #3 ] Predicate [ #3.Singer_ID = #1.Singer_ID ] Output [ #1.Name , #3.Count_Star ]",
      "#1 = Scan Table [ stadium ] Distinct [ true ] Output [ Name ] ; #2 = Scan Table [ stadium ] Output [ Stadium_ID , Name ] ; #3 = Scan Table [ concert ] Predicate [ Year = 2014 ] Output [ Stadium_ID , Year ] ; #4 = Join [ #2 , #3 ] Predicate [ #3.Stadium_ID = #2.Stadium_ID ] Distinct [ true ] Output [ #2.Name ] ; #5 = Except [ #1 , #4 ] Predicate [ #1.Name = #4.Name ] Output [ #1.Name ]",
      "#1 = Scan Table [ stadium ] Predicate [ Capacity >= 5000 AND Capacity <= 10000 ] Output [ Location , Capacity , Name ]",
      "#1 = Scan Table [ stadium ] Output [ Stadium_ID , Name ] ; #2 = Scan Table [ concert ] Output [ Stadium_ID ] ; #3 = Join [ #1 , #2 ] Predicate [ #2.Stadium_ID = #1.Stadium_ID ] Output [ #2.Stadium_ID , #1.Name ] ; #4 = Aggregate [ #3 ] GroupBy [ Stadium_ID ] Output [ countstar AS Count_Star , Name ]",
      "#1 = Scan Table [ stadium ] Output [ Average , Capacity ] ; #2 = Aggregate [ #1 ] GroupBy [ Average ] Output [ Average , MAX(Capacity) AS Max_Capacity ]"
    ];

    const NEGATIVES: [&str; 14] = [
      "#1 = Scan Table [ stadium ] Output [ Name, Capacity, Stadium_ID ] ; #2 = Scan Table [ concert ] Predicate [ Year >= 2014 ] Output [ Stadium_ID, Year ] ; #3 = Join [ #1, #2 ] Predicate [ #2.Stadium_ID = #1.Stadium_ID ] Output [ #1.Name, #1.Capacity ] ; #4 = Aggregate [ #3 ] GroupBy [ Name ] Output [ Name, countstar AS Count_Star ] ; #5 = TopSort [ #4 ] Rows [ 1 ] OrderBy [ Count_Star DESC ] Output [ Name, Count_Star, Capacity ]",
      "#1 = Scan Table [ stadium ] Output [ Location, Capacity, Name ] ; #2 = Aggregate [ #1 ] GroupBy [ Capacity ] Output [ Capacity, countstar AS Count_Star, Location ] ; #3 = Filter [ #2 ] Predicate [ Count_Star < 10000.0 ] Output [ Location, Count_Star, Name ]",
      "#1 = Scan Table [ concert ] Output [ Concert_Name, Theme ] ; #2 = Scan Table [ singer_in_concert ] Output [ Concert_ID, Singer_ID ] ; #3 = Join [ #1, #2 ] Predicate [ #2.Concert_ID = #1.Concert_ID ] Output [ #1.Concert_Name, #1.Theme ] ; #4 = Aggregate [ #3 ] GroupBy [ Concert_Name ] Output [ Concert_Name, countstar AS Count_Star ]",
      "#1 = Scan Table [ singer ] Output [ Age, Song_Name ] ; #2 = Aggregate [ #1 ] GroupBy [ Age ] Output [ Age, AVG(Age) AS Avg_Age ] ; #3 = TopSort [ #2 ] Rows [ 1 ] OrderBy [ Avg_Age DESC ] Output [ Age, Song_Name ]",
      "#1 = Scan Table [ concert ] Output [ Concert_Name, Theme, Concert_ID ] ; #2 = Scan Table [ singer_in_concert ] Output [ Concert_ID, Singer_ID ] ; #3 = Join [ #1, #2 ] Predicate [ #2.Concert_ID = #1.Concert_ID ] Output [ #1.Concert_Name, #2.Theme, #1.Concert_ID ] ; #4 = Aggregate [ #3 ] GroupBy [ Concert_Name ] Output [ Concert_Name, countstar AS Count_Star, Concert_Name ]",
      "#1 = Scan Table [ singer ] Output [ Name, Singer_ID ] ; #2 = Scan Table [ concert ] Predicate [ Year = 2014 ] Output [ Year, Concert_ID ] ; #3 = Join [ #1, #2 ] Predicate [ #2.Concert_ID = #1.Concert_ID ] Output [ #2.Name ]",
      "#1 = Scan Table [ singer ] Output [ Song_Name, Age ] ; #2 = TopSort [ #1 ] Rows [ 1 ] OrderBy [ Age DESC ] Output [ Song_Name, Age, Song_Release_Year ]",
      "#1 = Scan Table [ stadium ] Output [ Name, Location, Stadium_ID ] ; #2 = Scan Table [ concert ] Predicate [ Year = 2014 OR Year = 2015 ] Output [ Stadium_ID, Year ] ; #3 = Join [ #1, #2 ] Predicate [ #2.Stadium_ID = #1.Stadium_ID ] Output [ #1.Name, #2.Location ]",
      "#1 = Scan Table [ singer ] Output [ Age, Song_Name ] ; #2 = Aggregate [ #1 ] GroupBy [ Age ] Output [ Age, AVG(Age) AS Avg_Age ] ; #3 = Filter [ #2 ] Predicate [ Avg_Age >= 1 ] Output [ Song_Name ]",
      "#1 = Scan Table [ singer ] Output [ Name, Singer_ID ] ; #2 = Scan Table [ concert ] Predicate [ Year = 2014 ] Output [ Year, Concert_ID ] ; #3 = Scan Table [ singer_in_concert ] Output [ Singer_ID ] ; #4 = Join [ #2, #3 ] Predicate [ #3.Singer_ID = #2.Singer_ID ] Output [ #3.Name ]",
      "#1 = Scan Table [ stadium ] Output [ Location, Name, Stadium_ID ] ; #2 = Scan Table [ concert ] Predicate [ Year = 2014 AND Year = 2015 ] Output [ Stadium_ID, Year ] ; #3 = Join [ #1, #2 ] Predicate [ #2.Stadium_ID = #1.Stadium_ID ] Output [ #2.Name, #1.Location ]",
      "#1 = Scan Table [ stadium ] Output [ Name, Capacity, Stadium_ID ] ; #2 = Scan Table [ concert ] Predicate [ Year > 2013 ] Output [ Stadium_ID, Year ] ; #3 = Join [ #1, #2 ] Predicate [ #2.Stadium_ID = #1.Stadium_ID ] Output [ #1.Name, #1.Capacity ] ; #4 = Aggregate [ #3 ] GroupBy [ Name ] Output [ Name, countstar AS Count_Star ] ; #5 = TopSort [ #4 ] Rows [ 1 ] OrderBy [ Count_Star DESC ] Output [ Name, Count_Star, Capacity ]",
      "#1 = Scan Table [ stadium ] Output [ Capacity, Location, Name ] ; #2 = Aggregate [ #1 ] GroupBy [ Capacity ] Output [ Capacity, countstar AS Count_Star, Location ] ; #3 = Filter [ #2 ] Predicate [ Count_Star < 10000.0 ] Output [ Location, Name, Count_Star, Location, Name, Count_Star, Location, Count_Star, Location, Name, Count_Star, Location, Count_Star, Location, Count_Star, Location, Count_Star, Location, Count_Star, Location, Count_Star, Location, Count_Star, Location, Count_Star, Location, Count_Star, Location, Count_Star, Location, Count_Star, Location, Count_Star, Location, Count_Star, Location, Count_Star, Location, Count_Star, Location, Count_Star, Location, Count_Star, Location, Count_Star, Location, Count_Star, Location, Count_Star, Location, Count_Star, Location, Count_Star, Location, Count_Star, Location, Count_Star, Location, Count_Star, Location, Count_Star, Location, Count_Star, Location, Count_Star, Location, Count_Star, Location, Count_Star, Location, Count_Star, Location, Count_Star, Location, Count_Star, Location, Count_Star, Location, Count_Star, Location, Count_Star, Location, Count_Star, Location, Count_Star, Location, Count_Star, Location, Count_Star, Location, Count_Star, Count_Star, Location, Count_Star",
      "#1 = Scan Table [ singer ] Output [ Age, Song_Name ] ; #2 = Aggregate [ #1 ] GroupBy [ Age ] Output [ Age, AVG(Age) AS Avg_Age ] ; #3 = TopSort [ #2 ] Rows [ 1 ] OrderBy [ Avg_Age DESC ] Output [ Song_Name, Avg_Age, Affect_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_Sort_"
    ];

    #[test]
    fn test_single_line_qpl() {
        let mut input = get_input("#1 = Scan Table [ stadium ] Output [ Location ]");
        let _ = input.complete();
        let output = qpl::<ContextError>(true).parse_next(&mut input).unwrap();
        assert_eq!(
            output,
            vec![Line {
                idx: 1,
                operation: Operation::Scan {
                    table: "stadium".to_owned(),
                    predicate: None,
                    is_distinct: false
                }
            }]
        )
    }

    #[test]
    fn test_two_lines_qpl() {
        let mut input = get_input("#1 = Scan Table [ singer ] Output [ Age ] ; #2 = Aggregate [ #1 ] GroupBy [ Age ] Output [ countstar AS Count_Star ]");
        let _ = input.complete();
        let output = qpl::<ContextError>(true).parse_next(&mut input).unwrap();
        assert_eq!(
            output,
            vec![
                Line {
                    idx: 1,
                    operation: Operation::Scan {
                        table: "singer".to_owned(),
                        predicate: None,
                        is_distinct: false
                    },
                },
                Line {
                    idx: 2,
                    operation: Operation::Aggregate {
                        input: 1,
                        group_by: vec![String::from("Age")]
                    }
                }
            ]
        )
    }

    #[test]
    fn test_all_positives() {
        for example in POSITIVES {
            let mut input = get_input(example);
            let _ = input.complete();
            let result = qpl::<ContextError>(true).parse_next(&mut input);
            assert!(result.is_ok())
        }
    }

    #[test]
    fn test_all_negatives() {
        for example in NEGATIVES {
            let mut input = get_input(example);
            let _ = input.complete();
            let result = qpl::<ContextError>(true).parse_next(&mut input);
            assert!(result.is_err());
        }
    }

    #[test]
    fn test_partial_qpl() {
        let mut input = get_input("#1 = Scan Table [ stadium ] Output [ Name, Capacity, Stadium_ID ] ; #2 = Scan Table [ concert ] Predicate [ Year >= 2014 ] Output [ Stadium_ID, Year ] ; #3 = Join [ #1, #2 ] Predicate [ #2.Stadium_ID = #1.Stadium_ID ] Output [ #1.Name, #1.Capacity ] ; #4 = Aggregate [ #3 ] GroupBy [ Name ] Output [ Name, countstar AS Count_Star ] ; #5 = TopSort [ #4 ] Rows [ 1 ] OrderBy [ Count_Star ");
        let result = qpl::<ContextError>(true).parse_next(&mut input);
        assert!(matches!(result, Err(ErrMode::Incomplete(_))));
    }
}
