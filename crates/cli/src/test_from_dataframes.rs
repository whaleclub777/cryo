// Simple test for FromDataFrames derive macro
use cryo_freeze::{Datatype, CollectError, columns::FromDataFrames};
use cryo_to_df::FromDataFrames;
use polars::prelude::*;
use std::collections::HashMap;

#[derive(Default, FromDataFrames, Debug)]
pub struct SimpleTestStruct {
    pub n_rows: u64,
    pub field1: Vec<u32>,
    pub field2: Vec<String>,
    pub chain_id: Vec<u64>,
}

#[test]
fn test_from_dataframes_derive() {
    test_from_dataframes().unwrap();
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