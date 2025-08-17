from collections import Callable
from typing import (
    TYPE_CHECKING,
    Optional,
    Union,
    Sequence,
    Any,
    Dict,
    Type
)
from eth_typing.evm import Address, ChecksumAddress
from hexbytes import HexBytes
from web3 import Web3
from web3._utils.empty import empty
from web3.method import Method
from web3.module import Module
from web3.providers.base import BaseProvider
from web3.types import  RPCEndpoint
from ens import (
    ENS,
)

if TYPE_CHECKING:
    from web3._utils.empty import Empty  # noqa: F401

class Anvil(Module):
    w3: "Web3"

    _set_chain_id: Method[Callable[[int], None]] = Method(
        RPCEndpoint("anvil_setChainId")
    )

    def set_chain_id(self, chain_id: int):
        return self._set_chain_id(chain_id)

    _set_code: Method[Callable[[Union[Address, ChecksumAddress, ENS], HexBytes], None]] = Method(
        RPCEndpoint("anvil_setCode")
    )

    def set_code(
        self, account: Union[Address, ChecksumAddress, ENS], code: HexBytes
    ):
        return self._set_code(account, code)


class AnvilWeb3(Web3):
    anvil: Anvil  # Type annotation for the anvil module

    def __init__(
        self,
        provider: Optional[BaseProvider] = None,
        middleware: Optional[Sequence[Any]] = None,
        modules: Optional[Dict[str, Union[Type[Module], Sequence[Any]]]] = None,
        external_modules: Optional[
            Dict[str, Union[Type[Module], Sequence[Any]]]
        ] = None,
        ens: Union[ENS, "Empty"] = empty,
    ) -> None:
        super().__init__(provider, middleware, modules, external_modules, ens)
        self.attach_modules({
            "anvil": Anvil
        })
