from itertools import chain
from eth_typing.evm import ChecksumAddress
from web3 import Web3
from web3.method import Method
from web3.types import RPCEndpoint

w3 = Web3(Web3.HTTPProvider(endpoint_uri='http://127.0.0.1:8545'))
w3.eth.attach_methods({
    "anvil_setChainId": Method(
        json_rpc_method=RPCEndpoint("anvil_setChainId"),
    ),
    "anvil_setCode": Method(
        json_rpc_method=RPCEndpoint("anvil_setCode"),
    )
})
