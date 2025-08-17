from hexbytes.main import HexBytes
from web3 import Web3
from anvil import AnvilWeb3
from eth_abi import abi

# event Test(uint256 indexed number, uint256 data)
w3 = AnvilWeb3(Web3.HTTPProvider(endpoint_uri='http://127.0.0.1:8545'))
target = Web3.to_checksum_address('0xF2c77FdfDe6947a9AB51A372ac6a9fBff226F4d5')
test_account = w3.eth.account.from_key(0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80)

emiter_abi = '[{"type":"function","name":"emitEvent","inputs":[{"name":"selector","type":"bytes32","internalType":"bytes32"},{"name":"topic0","type":"bytes32","internalType":"bytes32"},{"name":"topic1","type":"bytes32","internalType":"bytes32"},{"name":"topic2","type":"bytes32","internalType":"bytes32"},{"name":"data","type":"bytes","internalType":"bytes"}],"outputs":[],"stateMutability":"nonpayable"},{"type":"function","name":"emitEvent","inputs":[{"name":"selector","type":"bytes32","internalType":"bytes32"},{"name":"topic0","type":"bytes32","internalType":"bytes32"},{"name":"data","type":"bytes","internalType":"bytes"}],"outputs":[],"stateMutability":"nonpayable"},{"type":"function","name":"emitEvent","inputs":[{"name":"selector","type":"bytes32","internalType":"bytes32"},{"name":"topic0","type":"bytes32","internalType":"bytes32"},{"name":"topic1","type":"bytes32","internalType":"bytes32"},{"name":"data","type":"bytes","internalType":"bytes"}],"outputs":[],"stateMutability":"nonpayable"},{"type":"function","name":"emitEvent","inputs":[{"name":"selector","type":"bytes32","internalType":"bytes32"},{"name":"data","type":"bytes","internalType":"bytes"}],"outputs":[],"stateMutability":"nonpayable"},{"type":"event","name":"Log1Event","inputs":[{"name":"data","type":"bytes","indexed":false,"internalType":"bytes"}],"anonymous":false}]'

eventer_bytescode = HexBytes.fromhex(
    "608060405234801561000f575f5ffd5b506004361061004a575f3560e01c80633e2738811461004e578063bc36b6101461006a578063e4e8440f14610086578063ee1efdb4146100a2575b5f5ffd5b610068600480360381019061006391906101a2565b6100be565b005b610084600480360381019061007f9190610238565b6100d3565b005b6100a0600480360381019061009b91906102a9565b6100e4565b005b6100bc60048036038101906100b7919061032d565b6100f7565b005b8080835f3783858789845fa450505050505050565b8080835f378385825fa25050505050565b8080835f37838587835fa3505050505050565b8080835f3783815fa150505050565b5f5ffd5b5f5ffd5b5f819050919050565b6101208161010e565b811461012a575f5ffd5b50565b5f8135905061013b81610117565b92915050565b5f5ffd5b5f5ffd5b5f5ffd5b5f5f83601f84011261016257610161610141565b5b8235905067ffffffffffffffff81111561017f5761017e610145565b5b60208301915083600182028301111561019b5761019a610149565b5b9250929050565b5f5f5f5f5f5f60a087890312156101bc576101bb610106565b5b5f6101c989828a0161012d565b96505060206101da89828a0161012d565b95505060406101eb89828a0161012d565b94505060606101fc89828a0161012d565b935050608087013567ffffffffffffffff81111561021d5761021c61010a565b5b61022989828a0161014d565b92509250509295509295509295565b5f5f5f5f606085870312156102505761024f610106565b5b5f61025d8782880161012d565b945050602061026e8782880161012d565b935050604085013567ffffffffffffffff81111561028f5761028e61010a565b5b61029b8782880161014d565b925092505092959194509250565b5f5f5f5f5f608086880312156102c2576102c1610106565b5b5f6102cf8882890161012d565b95505060206102e08882890161012d565b94505060406102f18882890161012d565b935050606086013567ffffffffffffffff8111156103125761031161010a565b5b61031e8882890161014d565b92509250509295509295909350565b5f5f5f6040848603121561034457610343610106565b5b5f6103518682870161012d565b935050602084013567ffffffffffffffff8111156103725761037161010a565b5b61037e8682870161014d565b9250925050925092509256fea2646970667358221220b78e2b289da00ba37966e0fb0db7dce07205eff308782c7bbc2fd6691cab65ec64736f6c634300081e0033"
)
eventer = w3.eth.contract(address=target, abi=emiter_abi)
w3.anvil.set_code(target, eventer_bytescode)

unset_emit_tx = eventer.functions.emitEvent(
    "0x91916a5e2c96453ddf6b585497262675140eb9f7a774095fb003d93e6dc69216",
    abi.encode(["uint256"], [12345])
).transact()

print(unset_emit_tx.to_0x_hex())
