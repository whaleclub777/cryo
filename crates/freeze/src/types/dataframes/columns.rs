use alloy::{
    dyn_abi::DynSolValue,
    hex::ToHexExt,
    primitives::{I256, U256},
};
use polars::{
    prelude::{Column, NamedFrom},
    series::Series,
};

use crate::{err, CollectError, ColumnEncoding, ColumnType, RawBytes, ToU256Series, U256Type};

/// A vector that can hold either values or optional values.
pub enum OptionVec<T> {
    /// A vector of values
    Some(Vec<T>),
    /// A vector of optional values
    Option(Vec<Option<T>>),
}

impl<T> OptionVec<T> {
    /// Returns whether the vector is empty.
    pub fn is_empty(&self) -> bool {
        match self {
            OptionVec::Some(v) => v.is_empty(),
            OptionVec::Option(v) => v.is_empty(),
        }
    }

    /// Get the length of the vector
    pub fn len(&self) -> usize {
        match self {
            OptionVec::Some(v) => v.len(),
            OptionVec::Option(v) => v.len(),
        }
    }

    /// Convert into a polars Column
    pub fn into_column<P1: ?Sized, P2: ?Sized>(self, name: String) -> Column
    where
        Series: NamedFrom<Vec<T>, P1>,
        Series: NamedFrom<Vec<Option<T>>, P2>,
    {
        match self {
            OptionVec::Some(v) => Column::new(name.into(), v),
            OptionVec::Option(v) => Column::new(name.into(), v),
        }
    }

    /// Convert into a variety of u256 Column representations
    pub fn into_u256_columns(
        self,
        name: String,
        u256_types: &[U256Type],
        column_encoding: ColumnEncoding,
    ) -> Result<Vec<Column>, CollectError>
    where
        Vec<T>: ToU256Series,
        Vec<Option<T>>: ToU256Series,
    {
        match self {
            OptionVec::Some(v) => {
                let mut series_vec = Vec::new();
                for u256_type in u256_types.iter() {
                    series_vec.push(v.to_u256_series(
                        name.to_string(),
                        u256_type.clone(),
                        column_encoding,
                    )?)
                }
                Ok(series_vec)
            }
            OptionVec::Option(v) => {
                let mut series_vec = Vec::new();
                for u256_type in u256_types.iter() {
                    series_vec.push(v.to_u256_series(
                        name.to_string(),
                        u256_type.clone(),
                        column_encoding,
                    )?)
                }
                Ok(series_vec)
            }
        }
    }
}

/// A collection of dynamic values that can be used in a DataFrame column.
pub enum DynValues {
    /// int
    Ints(OptionVec<i64>),
    /// uint
    UInts(OptionVec<u64>),
    /// u256
    U256s(OptionVec<U256>),
    /// i256
    I256s(OptionVec<I256>),
    /// bytes
    Bytes(OptionVec<RawBytes>),
    /// hex
    Hexes(OptionVec<String>),
    /// bool
    Bools(OptionVec<bool>),
    /// string
    Strings(OptionVec<String>),
}

macro_rules! impl_from_vec {
    ($($ty:ty => $variant:ident,)+) => {
        $(impl From<Vec<$ty>> for DynValues {
            fn from(value: Vec<$ty>) -> Self {
                DynValues::$variant(OptionVec::Some(value))
            }
        }
        impl From<Vec<Option<$ty>>> for DynValues {
            fn from(value: Vec<Option<$ty>>) -> Self {
                DynValues::$variant(OptionVec::Option(value))
            }
        })+
    };
}

impl_from_vec! {
    i64 => Ints,
    u64 => UInts,
    U256 => U256s,
    I256 => I256s,
    RawBytes => Bytes,
    String => Strings,
    bool => Bools,
}

impl DynValues {
    /// Create a new `DynValues` instance from a vector of `DynSolValue`s.
    pub fn from_sol_values(data: Vec<DynSolValue>, column_encoding: ColumnEncoding) -> Self {
        // This is a smooth brain way of doing this, but I can't think of a better way right now
        let mut ints: Vec<i64> = vec![];
        let mut uints: Vec<u64> = vec![];
        let mut u256s: Vec<U256> = vec![];
        let mut i256s: Vec<I256> = vec![];
        let mut bytes: Vec<RawBytes> = vec![];
        let mut hexes: Vec<String> = vec![];
        let mut bools: Vec<bool> = vec![];
        let mut strings: Vec<String> = vec![];
        // TODO: support array & tuple types

        for token in data {
            match token {
                DynSolValue::Address(a) => match column_encoding {
                    ColumnEncoding::Binary => bytes.push(a.to_vec()),
                    ColumnEncoding::Hex(with_prefix) => hexes.push(to_hex(a, with_prefix)),
                },
                DynSolValue::FixedBytes(b, _) => match column_encoding {
                    ColumnEncoding::Binary => bytes.push(b.to_vec()),
                    ColumnEncoding::Hex(with_prefix) => hexes.push(to_hex(b, with_prefix)),
                },
                DynSolValue::Bytes(b) => match column_encoding {
                    ColumnEncoding::Binary => bytes.push(b),
                    ColumnEncoding::Hex(with_prefix) => hexes.push(to_hex(b, with_prefix)),
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
            DynValues::Ints(OptionVec::Some(ints))
        } else if !i256s.is_empty() {
            DynValues::I256s(OptionVec::Some(i256s))
        } else if !u256s.is_empty() {
            DynValues::U256s(OptionVec::Some(u256s))
        } else if !uints.is_empty() {
            DynValues::UInts(OptionVec::Some(uints))
        } else if !bytes.is_empty() {
            DynValues::Bytes(OptionVec::Some(bytes))
        } else if !hexes.is_empty() {
            DynValues::Hexes(OptionVec::Some(hexes))
        } else if !bools.is_empty() {
            DynValues::Bools(OptionVec::Some(bools))
        } else if !strings.is_empty() {
            DynValues::Strings(OptionVec::Some(strings))
        } else {
            // case where no data was passed
            DynValues::UInts(OptionVec::Option(vec![]))
        }
    }

    /// Returns whether the underlying data is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
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
        u256_types: &[U256Type],
        column_encoding: ColumnEncoding,
    ) -> Result<Vec<Column>, CollectError> {
        match self {
            Self::Ints(ints) => Ok(vec![ints.into_column(name)]),
            Self::UInts(uints) => Ok(vec![uints.into_column(name)]),
            Self::I256s(i256s) => i256s.into_u256_columns(name, u256_types, column_encoding),
            Self::U256s(u256s) => u256s.into_u256_columns(name, u256_types, column_encoding),
            Self::Bytes(bytes) => {
                match column_encoding {
                    ColumnEncoding::Binary => Ok(vec![bytes.into_column(name)]),
                    ColumnEncoding::Hex(_) => Ok(vec![bytes.into_column(name)]),
                }
            }
            Self::Hexes(hexes) => Ok(vec![hexes.into_column(name)]),
            Self::Bools(bools) => Ok(vec![bools.into_column(name)]),
            Self::Strings(strings) => Ok(vec![strings.into_column(name)]),
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
        column_encoding: ColumnEncoding,
    ) -> Result<Vec<Column>, CollectError> {
        let values = DynValues::from_sol_values(data, column_encoding);
        let mixed_length_err = format!("could not parse column {name}, mixed type");
        let mixed_length_err = mixed_length_err.as_str();

        if values.len() != chunk_len {
            return Err(err(mixed_length_err))
        }
        values.into_columns(name, u256_types, column_encoding)
    }

    /// data should never be mixed type, otherwise this will return inconsistent results
    pub fn create_empty_columns(
        self,
        name: &str,
        u256_types: &[U256Type],
        column_encoding: ColumnEncoding,
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
        column_encoding: ColumnEncoding,
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
            ColumnType::UInt256 => {
                Column::new(format!("{name}_u256binary").into(), Vec::<RawBytes>::new())
            }
            ColumnType::Int32 => Column::new(name.into(), Vec::<i32>::new()),
            ColumnType::Int64 => Column::new(name.into(), Vec::<i64>::new()),
            ColumnType::Int256 => {
                Column::new(format!("{name}_i256binary").into(), Vec::<RawBytes>::new())
            }
            ColumnType::Float32 => Column::new(name.into(), Vec::<f32>::new()),
            ColumnType::Float64 => Column::new(name.into(), Vec::<f64>::new()),
            ColumnType::Decimal128 => Column::new(name.into(), Vec::<RawBytes>::new()),
            ColumnType::String => Column::new(name.into(), Vec::<String>::new()),
            ColumnType::Binary => Column::new(name.into(), Vec::<RawBytes>::new()),
            ColumnType::Hex => Column::new(name.into(), Vec::<String>::new()),
        }
    }
}

fn to_hex<T: ToHexExt>(value: T, with_prefix: bool) -> String {
    if with_prefix {
        value.encode_hex_with_prefix()
    } else {
        value.encode_hex()
    }
}
