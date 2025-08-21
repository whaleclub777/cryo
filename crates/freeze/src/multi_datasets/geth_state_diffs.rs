use crate::*;
use alloy::{
    primitives::{Address, Bytes, B256, U256},
    rpc::types::trace::geth::{AccountState, DiffMode},
};
use polars::prelude::*;
use std::collections::{BTreeMap, HashMap, HashSet};

/// state diffs from geth debug traces
pub struct GethStateDiffs(
    pub Option<GethBalanceDiffs>,
    pub Option<GethCodeDiffs>,
    pub Option<GethNonceDiffs>,
    pub Option<GethStorageDiffs>,
);

impl Default for GethStateDiffs {
    fn default() -> GethStateDiffs {
        GethStateDiffs(
            Some(GethBalanceDiffs::default()),
            Some(GethCodeDiffs::default()),
            Some(GethNonceDiffs::default()),
            Some(GethStorageDiffs::default()),
        )
    }
}

impl ToDataFrames for GethStateDiffs {
    fn create_dfs(
        self,
        schemas: &HashMap<Datatype, Table>,
        chain_id: u64,
    ) -> R<HashMap<Datatype, DataFrame>> {
        let GethStateDiffs(balance_diffs, code_diffs, nonce_diffs, storage_diffs) = self;
        let mut output = HashMap::new();
        if let Some(balance_diffs) = balance_diffs {
            output.extend(balance_diffs.create_dfs(schemas, chain_id)?);
        }
        if let Some(code_diffs) = code_diffs {
            output.extend(code_diffs.create_dfs(schemas, chain_id)?);
        }
        if let Some(nonce_diffs) = nonce_diffs {
            output.extend(nonce_diffs.create_dfs(schemas, chain_id)?);
        }
        if let Some(storage_diffs) = storage_diffs {
            output.extend(storage_diffs.create_dfs(schemas, chain_id)?);
        }
        Ok(output)
    }
}

type BlockTxsTraces = (Option<u32>, Vec<Option<RawBytes>>, Vec<DiffMode>);

#[async_trait::async_trait]
impl CollectByBlock for GethStateDiffs {
    type Response = BlockTxsTraces;

    async fn extract(request: Params, source: Arc<Source>, query: Arc<Query>) -> R<Self::Response> {
        let block_number = request.block_number()? as u32;
        let include_txs = query.schemas.values().any(|x| x.has_column("transaction_hash"));
        source.geth_debug_trace_block_diffs(block_number, include_txs).await
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let GethStateDiffs(ref mut balances, ref mut codes, ref mut nonces, ref mut storages) =
            columns;
        process_geth_diffs(
            &response,
            balances.as_mut(),
            codes.as_mut(),
            nonces.as_mut(),
            storages.as_mut(),
            &query.schemas,
        )
    }
}

#[async_trait::async_trait]
impl CollectByTransaction for GethStateDiffs {
    type Response = BlockTxsTraces;

    async fn extract(request: Params, source: Arc<Source>, query: Arc<Query>) -> R<Self::Response> {
        let include_block_number = query.schemas.values().any(|x| x.has_column("transaction_hash"));
        source
            .geth_debug_trace_transaction_diffs(request.transaction_hash()?, include_block_number)
            .await
    }

    fn transform(response: Self::Response, columns: &mut Self, query: &Arc<Query>) -> R<()> {
        let GethStateDiffs(ref mut balances, ref mut codes, ref mut nonces, ref mut storages) =
            columns;
        process_geth_diffs(
            &response,
            balances.as_mut(),
            codes.as_mut(),
            nonces.as_mut(),
            storages.as_mut(),
            &query.schemas,
        )
    }
}

pub(crate) fn process_geth_diffs(
    response: &BlockTxsTraces,
    mut balances: Option<&mut GethBalanceDiffs>,
    mut codes: Option<&mut GethCodeDiffs>,
    mut nonces: Option<&mut GethNonceDiffs>,
    mut storages: Option<&mut GethStorageDiffs>,
    schemas: &Schemas,
) -> R<()> {
    let (block_number, txs, traces) = response;
    let balance_schema = schemas.get(&Datatype::GethBalanceDiffs);
    let code_schema = schemas.get(&Datatype::GethCodeDiffs);
    let nonce_schema = schemas.get(&Datatype::GethNonceDiffs);
    let storage_schema = schemas.get(&Datatype::GethStorageDiffs);

    let blank = &AccountState::default();
    for (tx_index, (trace, tx)) in traces.iter().zip(txs).enumerate() {
        let index = &(*block_number, tx_index as u32, tx.clone());
        let addresses: Vec<_> = trace
            .pre
            .keys()
            .chain(trace.post.keys())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        for address in addresses.into_iter() {
            let (pre, post) = match (trace.pre.get(address), trace.post.get(address)) {
                (Some(pre), Some(post)) => (pre, post),
                (Some(pre), None) => (pre, blank),
                (None, Some(post)) => (blank, post),
                (None, None) => (blank, blank),
            };
            if let (Some(balances), Some(schema)) = (balances.as_mut(), balance_schema) {
                add_balances(address, pre.balance, post.balance, balances, schema, index)?;
            }
            if let (Some(codes), Some(schema)) = (codes.as_mut(), code_schema) {
                add_codes(address, &pre.code, &post.code, codes, schema, index)?;
            }
            if let (Some(nonces), Some(schema)) = (nonces.as_mut(), nonce_schema) {
                add_nonces(address, pre.nonce, post.nonce, nonces, schema, index)?;
            }
            if let (Some(storages), Some(schema)) = (storages.as_mut(), storage_schema) {
                add_storages(address, &pre.storage, &post.storage, storages, schema, index)?;
            }
        }
    }
    Ok(())
}

fn add_balances(
    address: &Address,
    pre: Option<U256>,
    post: Option<U256>,
    columns: &mut GethBalanceDiffs,
    schema: &Table,
    index: &(Option<u32>, u32, Option<RawBytes>),
) -> R<()> {
    let (from_value, to_value) = parse_pre_post(pre, post, U256::ZERO);
    let (block_number, transaction_index, transaction_hash) = index;
    columns.n_rows += 1;
    store!(schema, columns, block_number, *block_number);
    store!(schema, columns, transaction_index, Some(*transaction_index as u64));
    store!(schema, columns, transaction_hash, transaction_hash.clone());
    store!(schema, columns, address, address.to_vec());
    store!(schema, columns, from_value, from_value);
    store!(schema, columns, to_value, to_value);
    Ok(())
}

fn add_codes(
    address: &Address,
    pre: &Option<Bytes>,
    post: &Option<Bytes>,
    columns: &mut GethCodeDiffs,
    schema: &Table,
    index: &(Option<u32>, u32, Option<RawBytes>),
) -> R<()> {
    let blank = Bytes::new();
    let (from_value, to_value) = match (pre, post) {
        (Some(pre), Some(post)) => (pre, post),
        (Some(pre), None) => (pre, &blank),
        (None, Some(post)) => (&blank, post),
        (None, None) => (&blank, &blank),
    };
    let (block_number, transaction_index, transaction_hash) = index;
    columns.n_rows += 1;
    store!(schema, columns, block_number, *block_number);
    store!(schema, columns, transaction_index, Some(*transaction_index as u64));
    store!(schema, columns, transaction_hash, transaction_hash.clone());
    store!(schema, columns, address, address.to_vec());
    store!(schema, columns, from_value, from_value.to_vec());
    store!(schema, columns, to_value, to_value.to_vec());
    Ok(())
}

fn add_nonces(
    address: &Address,
    pre: Option<u64>,
    post: Option<u64>,
    columns: &mut GethNonceDiffs,
    schema: &Table,
    index: &(Option<u32>, u32, Option<RawBytes>),
) -> R<()> {
    let (from_value, to_value) = parse_pre_post(pre, post, 0_u64);
    let (block_number, transaction_index, transaction_hash) = index;
    columns.n_rows += 1;
    store!(schema, columns, block_number, *block_number);
    store!(schema, columns, transaction_index, Some(*transaction_index as u64));
    store!(schema, columns, transaction_hash, transaction_hash.clone());
    store!(schema, columns, address, address.to_vec());
    store!(schema, columns, from_value, from_value);
    store!(schema, columns, to_value, to_value);
    Ok(())
}

fn add_storages(
    address: &Address,
    pre: &BTreeMap<B256, B256>,
    post: &BTreeMap<B256, B256>,
    columns: &mut GethStorageDiffs,
    schema: &Table,
    index: &(Option<u32>, u32, Option<RawBytes>),
) -> R<()> {
    let (block_number, transaction_index, transaction_hash) = index;
    let slots: Vec<_> = pre
        .clone()
        .into_keys()
        .chain(post.clone().into_keys())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let blank = B256::ZERO;
    for slot in slots.into_iter() {
        let (from, to) = match (pre.get(&slot), post.get(&slot)) {
            (Some(pre), Some(post)) => (pre, post),
            (Some(pre), None) => (pre, &blank),
            (None, Some(post)) => (&blank, post),
            (None, None) => (&blank, &blank),
        };
        columns.n_rows += 1;
        store!(schema, columns, block_number, *block_number);
        store!(schema, columns, transaction_index, Some(*transaction_index as u64));
        store!(schema, columns, transaction_hash, transaction_hash.clone());
        store!(schema, columns, address, address.to_vec());
        store!(schema, columns, slot, slot.to_vec());
        store!(schema, columns, from_value, from.to_vec());
        store!(schema, columns, to_value, to.to_vec());
    }
    Ok(())
}

fn parse_pre_post<T>(pre: Option<T>, post: Option<T>, new: T) -> (T, T)
where
    T: Copy + Clone,
{
    match (pre, post) {
        (Some(pre), Some(post)) => (pre, post),
        (Some(pre), None) => (pre, new),
        (None, Some(post)) => (new, post),
        (None, None) => (new, new),
    }
}
