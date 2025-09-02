// Simple test for FromDataFrames derive macro
use cryo_freeze::{Datatype, CollectError, columns::FromDataFrames};
use cryo_to_df::FromDataFrames;
use polars::prelude::*;
use std::collections::HashMap;
use alloy::primitives::{U256, I256};

#[derive(Default, FromDataFrames, Debug)]
pub struct SimpleTestStruct {
    pub n_rows: u64,
    pub field1: Vec<u32>,
    pub field2: Vec<String>,
    pub chain_id: Vec<u64>,
}

#[derive(Default, FromDataFrames, Debug)]
pub struct U256TestStruct {
    pub n_rows: u64,
    pub value_u256: Vec<U256>,
    pub value_i256: Vec<I256>,
    pub optional_u256: Vec<Option<U256>>,
    pub optional_i256: Vec<Option<I256>>,
    pub chain_id: Vec<u64>,
}

#[derive(Default, FromDataFrames, Debug)]
pub struct FallbackTestStruct {
    pub n_rows: u64,
    pub fallback_u256: Vec<U256>,
    pub fallback_i256: Vec<I256>,
    pub chain_id: Vec<u64>,
}

#[test]
fn test_from_dataframes_derive() {
    test_from_dataframes().unwrap();
}

#[test]
fn test_u256_i256_binary_columns() {
    test_u256_i256_from_dataframes().unwrap();
}

#[test]
fn test_binary_column_fallback() {
    test_binary_fallback().unwrap();
}

#[allow(dead_code)]
pub fn test_from_dataframes() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing FromDataFrames derive macro...");
    
    // Create test data
    let field1_data = vec![1u32, 2, 3];
    let field2_data = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    let chain_id_data = vec![1u64, 1, 1];

    // Create DataFrame
    let df = DataFrame::new(vec![
        Column::new("field1".into(), field1_data.clone()),
        Column::new("field2".into(), field2_data.clone()),
        Column::new("chain_id".into(), chain_id_data.clone()),
    ])?;

    // Create input for from_dfs (using an existing datatype)
    let mut dfs = HashMap::new();
    dfs.insert(Datatype::Blocks, df);

    // Call from_dfs
    let result = SimpleTestStruct::from_dfs(dfs, &Datatype::Blocks)?;

    println!("Result n_rows: {}", result.n_rows);
    println!("Result field1: {:?}", result.field1);
    println!("Result field2: {:?}", result.field2);
    println!("Result chain_id: {:?}", result.chain_id);
    
    assert_eq!(result.n_rows, 3);
    assert_eq!(result.field1, field1_data);
    assert_eq!(result.field2, field2_data);
    assert_eq!(result.chain_id, chain_id_data);
    
    println!("FromDataFrames derive macro works correctly!");
    Ok(())
}

#[allow(dead_code)]
pub fn test_u256_i256_from_dataframes() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing U256/I256 FromDataFrames with binary columns...");
    
    // Create test U256/I256 values
    let u256_val = U256::from(12345u64);
    let i256_val = I256::from_raw(U256::from(67890u64));
    
    // Convert to binary data (32 bytes each)
    let u256_binary = u256_val.to_be_bytes_vec();
    let i256_binary = i256_val.to_be_bytes::<32>().to_vec();
    
    println!("U256 binary length: {}", u256_binary.len());
    println!("I256 binary length: {}", i256_binary.len());
    
    let chain_id_data = vec![1u64, 1];

    // Create DataFrame with binary columns using the naming convention
    let df = DataFrame::new(vec![
        Column::new("value_u256_u256binary".into(), vec![u256_binary.clone(), u256_binary.clone()]),
        Column::new("value_i256_i256binary".into(), vec![i256_binary.clone(), i256_binary.clone()]),
        Column::new("optional_u256_u256binary".into(), vec![Some(u256_binary.clone()), None]),
        Column::new("optional_i256_i256binary".into(), vec![Some(i256_binary.clone()), None]),
        Column::new("chain_id".into(), chain_id_data.clone()),
    ])?;

    // Create input for from_dfs
    let mut dfs = HashMap::new();
    dfs.insert(Datatype::Blocks, df);

    // Call from_dfs
    let result = U256TestStruct::from_dfs(dfs, &Datatype::Blocks)?;

    println!("Result n_rows: {}", result.n_rows);
    println!("Result value_u256: {:?}", result.value_u256);
    println!("Result value_i256: {:?}", result.value_i256);
    println!("Result optional_u256: {:?}", result.optional_u256);
    println!("Result optional_i256: {:?}", result.optional_i256);
    
    assert_eq!(result.n_rows, 2);
    assert_eq!(result.value_u256, vec![u256_val, u256_val]);
    assert_eq!(result.value_i256, vec![i256_val, i256_val]);
    assert_eq!(result.optional_u256, vec![Some(u256_val), None]);
    assert_eq!(result.optional_i256, vec![Some(i256_val), None]);
    assert_eq!(result.chain_id, chain_id_data);
    
    println!("U256/I256 FromDataFrames works correctly!");
    Ok(())
}

#[allow(dead_code)]
pub fn test_binary_fallback() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing binary column name fallback behavior...");
    
    // Create test U256/I256 values
    let u256_val = U256::from(99999u64);
    let i256_val = I256::from_raw(U256::from(88888u64));
    
    // Convert to binary data (32 bytes each)
    let u256_binary = u256_val.to_be_bytes_vec();
    let i256_binary = i256_val.to_be_bytes::<32>().to_vec();
    
    println!("Testing fallback to _binary columns...");
    
    let chain_id_data = vec![1u64];

    // Create DataFrame with ONLY _binary columns (no _u256binary or _i256binary)
    // This should test the fallback behavior
    let df = DataFrame::new(vec![
        Column::new("fallback_u256_binary".into(), vec![u256_binary.clone()]),
        Column::new("fallback_i256_binary".into(), vec![i256_binary.clone()]),
        Column::new("chain_id".into(), chain_id_data.clone()),
    ])?;

    // Create input for from_dfs
    let mut dfs = HashMap::new();
    dfs.insert(Datatype::Blocks, df);

    // Call from_dfs - this should successfully use the _binary fallback columns
    let result = FallbackTestStruct::from_dfs(dfs, &Datatype::Blocks)?;

    println!("Result n_rows: {}", result.n_rows);
    println!("Result fallback_u256: {:?}", result.fallback_u256);
    println!("Result fallback_i256: {:?}", result.fallback_i256);
    
    assert_eq!(result.n_rows, 1);
    assert_eq!(result.fallback_u256, vec![u256_val]);
    assert_eq!(result.fallback_i256, vec![i256_val]);
    assert_eq!(result.chain_id, chain_id_data);
    
    println!("Binary column fallback works correctly!");
    Ok(())
}