#[cfg(test)]
use crate::domain::SqlSchema;

#[cfg(test)]
pub fn concert_singer() -> SqlSchema {
    use crate::domain::ColumnType::*;
    use std::collections::HashMap;

    let db_id = "concert_singer".to_owned();
    let table_names = vec!["stadium", "singer", "concert", "singer_in_concert"]
        .into_iter()
        .map(|s| s.to_owned())
        .collect::<Vec<_>>();
    let column_names = vec![
        "Stadium_ID",
        "Location",
        "Name",
        "Capacity",
        "Highest",
        "Lowest",
        "Average",
        "Singer_ID",
        "Name",
        "Country",
        "Song_Name",
        "Song_release_year",
        "Age",
        "Is_male",
        "concert_ID",
        "concert_Name",
        "Theme",
        "Stadium_ID",
        "Year",
        "concert_ID",
        "Singer_ID",
    ]
    .into_iter()
    .map(|s| s.to_owned())
    .collect::<Vec<_>>();
    let column_types = vec![
        Number, Text, Text, Number, Number, Number, Number, Number, Text, Text, Text, Text, Number,
        Others, Number, Text, Text, Number, Number, Number, Number,
    ];
    let column_to_table: Vec<usize> = vec![
        0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3,
    ];
    let table_to_columns: HashMap<String, Vec<usize>> = HashMap::from([
        ("stadium".to_owned(), (0..=6).collect()),
        ("singer".to_owned(), (7..=13).collect()),
        ("concert".to_owned(), (14..=18).collect()),
        ("singer_in_concert".to_owned(), vec![19, 20]),
    ]);
    let foreign_keys: Vec<(usize, usize)> = vec![(17, 0), (20, 7), (19, 14)];
    let primary_keys: Vec<usize> = vec![0, 7, 14, 19];

    SqlSchema::new(
        db_id,
        table_names,
        column_names,
        column_types,
        column_to_table,
        table_to_columns,
        foreign_keys,
        primary_keys,
    )
}
