/// convert a Vec to Column and add to Vec<Column>
#[macro_export]
macro_rules! with_column {
    ($all_columns:expr, $name:expr, $value:expr, $schema:expr) => {
        if $schema.has_column($name) {
            $all_columns.push(Column::new($name.into(), $value));
        }
    };
}

/// convert a Vec to Column, as hex if specified, and add to Vec<Column>
#[macro_export]
macro_rules! with_column_binary {
    ($all_columns:expr, $name:expr, $value:expr, $schema:expr) => {
        if let Some(col_type) = $schema.column_type($name) {
            match col_type {
                ColumnType::Hex => $all_columns
                    .push(Column::new($name.into(), $value.to_vec_hex($schema.config.hex_prefix))),
                _ => $all_columns.push(Column::new($name.into(), $value)),
            }
        }
    };
}

/// convert a Vec<U256> to variety of u256 Column representations
#[macro_export]
macro_rules! with_column_u256 {
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
