use crate::*;
use alloy::primitives::Address;
use polars::prelude::*;

/// columns for balances
#[derive(Default, cryo_to_df::ToDataFrames)]
pub struct Codes {
    n_rows: usize,
    block_number: Vec<u32>,
    address: Vec<RawBytes>,
    code: Vec<RawBytes>,
    chain_id: Vec<u64>,
}

#[async_trait::async_trait]
impl Dataset for Codes {
    fn default_sort() -> Option<Vec<&'static str>> {
        Some(vec!["block_number", "address"])
    }

    fn required_parameters() -> Vec<Dim> {
        vec![Dim::Address]
    }

    fn arg_aliases() -> Option<std::collections::HashMap<Dim, Dim>> {
        Some([(Dim::Contract, Dim::Address)].into_iter().collect())
    }

    fn default_blocks() -> Option<String> {
        Some("latest".to_string())
    }
}

type BlockTxAddressOutput = (u32, Option<RawBytes>, RawBytes, RawBytes);

#[async_trait::async_trait]
impl CollectByBlock for Codes {
    type Response = BlockTxAddressOutput;

    async fn extract(request: Params, source: Arc<Source>, _: Arc<Query>) -> R<Self::Response> {
        let address = request.address()?;
        let block_number = request.block_number()? as u32;
        let output = source.get_code(Address::from_slice(&address), block_number.into()).await?;
        Ok((block_number, None, address, output.to_vec()))
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let schema = query.schemas.get_schema(&Datatype::Codes)?;
        process_code(columns, response, schema)
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for Codes {
    type Response = ();
}

fn process_code(columns: &mut Codes, data: BlockTxAddressOutput, schema: &Table) -> R<()> {
    let (block, _tx, address, output) = data;
    columns.n_rows += 1;
    store!(schema, columns, block_number, block);
    store!(schema, columns, address, address);
    store!(schema, columns, code, output);
    Ok(())
}
