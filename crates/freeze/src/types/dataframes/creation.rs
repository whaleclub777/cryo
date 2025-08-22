/// convert a Vec to Column and add to Vec<Column>
#[macro_export]
macro_rules! with_column {
    ($all_columns:expr, $name:expr, $value:expr, $schema:expr) => {
        if let Some(col_type) = $schema.column_type($name) {
            let cols = DynValues::from($value).into_columns(
                $name.to_string(),
                col_type,
                &$schema.config,
            )?;
            $all_columns.extend(cols);
        }
    };
}
