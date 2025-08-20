use alloy::{dyn_abi::DynSolValue, hex::ToHexExt, primitives::{I256, U256}};
use polars::prelude::Column;

use crate::{err, CollectError, ColumnEncoding, ColumnType, ToU256Series, U256Type};

/// A collection of dynamic values that can be used in a DataFrame column.
pub enum DynValues {
    /// int
    Ints(Vec<i64>),
    /// uint
    UInts(Vec<u64>),
    /// u256
    U256s(Vec<U256>),
    /// i256
    I256s(Vec<I256>),
    /// bytes
    Bytes(Vec<Vec<u8>>),
    /// hex
    Hexes(Vec<String>),
    /// bool
    Bools(Vec<bool>),
    /// string
    Strings(Vec<String>),
}

impl DynValues {
    /// Create a new `DynValues` instance from a vector of `DynSolValue`s.
    pub fn from_sol_values(
        data: Vec<DynSolValue>,
        column_encoding: &ColumnEncoding,
    ) -> Self {
        // This is a smooth brain way of doing this, but I can't think of a better way right now
        let mut ints: Vec<i64> = vec![];
        let mut uints: Vec<u64> = vec![];
        let mut u256s: Vec<U256> = vec![];
        let mut i256s: Vec<I256> = vec![];
        let mut bytes: Vec<Vec<u8>> = vec![];
        let mut hexes: Vec<String> = vec![];
        let mut bools: Vec<bool> = vec![];
        let mut strings: Vec<String> = vec![];
        // TODO: support array & tuple types

        for token in data {
            match token {
                DynSolValue::Address(a) => match column_encoding {
                    ColumnEncoding::Binary => bytes.push(a.to_vec()),
                    ColumnEncoding::Hex => hexes.push(format!("{a:?}")),
                },
                DynSolValue::FixedBytes(b, _) => match column_encoding {
                    ColumnEncoding::Binary => bytes.push(b.to_vec()),
                    ColumnEncoding::Hex => hexes.push(b.encode_hex()),
                },
                DynSolValue::Bytes(b) => match column_encoding {
                    ColumnEncoding::Binary => bytes.push(b),
                    ColumnEncoding::Hex => hexes.push(b.encode_hex()),
                },
                DynSolValue::Uint(i, size) => {
                    if size <= 64 {
                        uints.push(i.wrapping_to::<u64>())
                    } else {
                        u256s.push(i)
                    }
                }
                DynSolValue::Int(i, size) => {
                    if size <= 64 {
                        ints.push(i.unchecked_into());
                    } else {
                        i256s.push(i);
                    }
                }
                DynSolValue::Bool(b) => bools.push(b),
                DynSolValue::String(s) => strings.push(s),
                DynSolValue::Array(_) | DynSolValue::FixedArray(_) => {}
                DynSolValue::Tuple(_) => {}
                DynSolValue::Function(_) => {}
            }
        }

        if !ints.is_empty() {
            DynValues::Ints(ints)
        } else if !i256s.is_empty() {
            DynValues::I256s(i256s)
        } else if !u256s.is_empty() {
            DynValues::U256s(u256s)
        } else if !uints.is_empty() {
            DynValues::UInts(uints)
        } else if !bytes.is_empty() {
            DynValues::Bytes(bytes)
        } else if !hexes.is_empty() {
            DynValues::Hexes(hexes)
        } else if !bools.is_empty() {
            DynValues::Bools(bools)
        } else if !strings.is_empty() {
            DynValues::Strings(strings)
        } else {
            // case where no data was passed
            DynValues::UInts(vec![])
        }
    }

    /// Returns the length of the underlying data.
    pub fn len(&self) -> usize {
        match self {
            DynValues::Ints(ints) => ints.len(),
            DynValues::I256s(i256s) => i256s.len(),
            DynValues::U256s(u256s) => u256s.len(),
            DynValues::UInts(uints) => uints.len(),
            DynValues::Bytes(bytes) => bytes.len(),
            DynValues::Hexes(hexes) => hexes.len(),
            DynValues::Bools(bools) => bools.len(),
            DynValues::Strings(strings) => strings.len(),
        }
    }

    /// Converts the `DynValues` into `Vec<Column>`.
    pub fn into_columns(
        self,
        name: String,
        chunk_len: usize,
        u256_types: &[U256Type],
        column_encoding: &ColumnEncoding,
    ) -> Result<Vec<Column>, CollectError> {
        let mixed_length_err = format!("could not parse column {name}, mixed type");
        let mixed_length_err = mixed_length_err.as_str();

        if self.len() != chunk_len {
            return Err(err(mixed_length_err))
        }
        match self {
            Self::Ints(ints) => {
                Ok(vec![Column::new(name.into(), ints)])
            }
            Self::UInts(uints) => {
                Ok(vec![Column::new(name.into(), uints)])
            }
            Self::I256s(i256s) => {
                let mut series_vec = Vec::new();
                for u256_type in u256_types.iter() {
                    series_vec.push(i256s.to_u256_series(
                        name.clone(),
                        u256_type.clone(),
                        column_encoding,
                    )?)
                }
                Ok(series_vec)
            }
            Self::U256s(u256s) => {
                let mut series_vec = Vec::new();
                for u256_type in u256_types.iter() {
                    series_vec.push(u256s.to_u256_series(
                        name.clone(),
                        u256_type.clone(),
                        column_encoding,
                    )?)
                }
                Ok(series_vec)
            }
            Self::Bytes(bytes) => {
                Ok(vec![Column::new(name.into(), bytes)])
            }
            Self::Hexes(hexes) => {
                Ok(vec![Column::new(name.into(), hexes)])
            }
            Self::Bools(bools) => {
                Ok(vec![Column::new(name.into(), bools)])
            }
            Self::Strings(strings) => {
                Ok(vec![Column::new(name.into(), strings)])
            }
        }
    }
}

impl ColumnType {
    /// data should never be mixed type, otherwise this will return inconsistent results
    pub fn create_column_from_values(
        name: String,
        data: Vec<DynSolValue>,
        chunk_len: usize,
        u256_types: &[U256Type],
        column_encoding: &ColumnEncoding,
    ) -> Result<Vec<Column>, CollectError> {
        let values = DynValues::from_sol_values(data, column_encoding);
        values.into_columns(name, chunk_len, u256_types, column_encoding)
    }

    /// data should never be mixed type, otherwise this will return inconsistent results
    pub fn create_empty_columns(
        self,
        name: &str,
        u256_types: &[U256Type],
        column_encoding: &ColumnEncoding,
    ) -> Vec<Column> {
        if self.is_256() {
            return self.create_empty_u256_columns(name, u256_types, column_encoding);
        }
        vec![self.create_single_empty_column(name)]
    }

    /// Create empty columns for U256 types
    pub fn create_empty_u256_columns(
        self,
        name: &str,
        u256_types: &[U256Type],
        column_encoding: &ColumnEncoding,
    ) -> Vec<Column> {
        u256_types
            .iter()
            .map(|u256_type| {
                let new_type = u256_type.to_columntype(column_encoding);
                let full_name = name.to_string() + u256_type.suffix(self).as_str();
                new_type.create_single_empty_column(&full_name)
            })
            .collect()
    }

    /// Create an empty column of the specified type
    pub fn create_single_empty_column(self, name: &str) -> Column {
        match self {
            ColumnType::Boolean => Column::new(name.into(), Vec::<bool>::new()),
            ColumnType::UInt32 => Column::new(name.into(), Vec::<u32>::new()),
            ColumnType::UInt64 => Column::new(name.into(), Vec::<u64>::new()),
            ColumnType::UInt256 => Column::new(format!("{name}_u256binary").into(), Vec::<Vec<u8>>::new()),
            ColumnType::Int32 => Column::new(name.into(), Vec::<i32>::new()),
            ColumnType::Int64 => Column::new(name.into(), Vec::<i64>::new()),
            ColumnType::Int256 => Column::new(format!("{name}_i256binary").into(), Vec::<Vec<u8>>::new()),
            ColumnType::Float32 => Column::new(name.into(), Vec::<f32>::new()),
            ColumnType::Float64 => Column::new(name.into(), Vec::<f64>::new()),
            ColumnType::Decimal128 => Column::new(name.into(), Vec::<Vec<u8>>::new()),
            ColumnType::String => Column::new(name.into(), Vec::<String>::new()),
            ColumnType::Binary => Column::new(name.into(), Vec::<Vec<u8>>::new()),
            ColumnType::Hex => Column::new(name.into(), Vec::<String>::new()),
        }
    }
}
