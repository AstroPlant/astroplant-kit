import logging
import abc
import asyncio
import json
from .schema import astroplant_capnp
from ..peripheral import PeripheralCommandResult

from typing import Any
from typing_extensions import Protocol

logger = logging.getLogger("astroplant_kit.api")


class Response(Protocol):
    def to_bytes_packed(self) -> bytes:
        ...


class KitRpcHandler(abc.ABC):
    """
    Abstract class representing the kit RPC handler interface.
    """

    @abc.abstractmethod
    async def version(self) -> str:
        pass

    @abc.abstractmethod
    async def uptime(self) -> int:
        pass

    @abc.abstractmethod
    async def peripheral_command(
        self, peripheral: str, command: str
    ) -> PeripheralCommandResult:
        pass

    @abc.abstractmethod
    async def peripheral_command_lock(self, peripheral: str) -> bool:
        pass


class KitRpc(object):
    """
    Handles MQTT messages to implement the kit RPC system.
    """

    def __init__(self, kit_rpc_response_handle):
        self._kit_rpc_response_handle = kit_rpc_response_handle
        self._handler = None

    def _register_handler(self, kit_rpc_handler: KitRpcHandler) -> None:
        self._handler = kit_rpc_handler

    def _send_response(self, response: Response) -> None:
        """
        Send an RPC response over MQTT.
        """
        self._kit_rpc_response_handle(response.to_bytes_packed())

    async def _handle_request(self, request: astroplant_capnp.KitRpcRequest) -> None:
        rpc = self._handler

        response = astroplant_capnp.KitRpcResponse.new_message(id=request.id)

        which = request.which()
        print(which)
        if which == "version":
            response.version = await rpc.version()
        elif which == "uptime":
            response.uptime = await rpc.uptime()
        elif which == "peripheralCommand":
            try:
                result = await rpc.peripheral_command(
                    request.peripheralCommand.peripheral,
                    json.loads(request.peripheralCommand.command),
                )
                if result is None:
                    peripheral_command_response = astroplant_capnp.KitRpcResponse.PeripheralCommand.new_message(
                        mediaType="text/plain",
                        data=b"error",
                        metadata=json.dumps(None),
                    )
                elif result.media is None:
                    peripheral_command_response = astroplant_capnp.KitRpcResponse.PeripheralCommand.new_message(
                        mediaType="text/plain", data=b"ok", metadata=json.dumps(None),
                    )
                else:
                    media = result.media
                    peripheral_command_response = astroplant_capnp.KitRpcResponse.PeripheralCommand.new_message(
                        mediaType=media.type,
                        data=media.data,
                        metadata=json.dumps(media.metadata),
                    )
                response.peripheralCommand = peripheral_command_response
            except:
                error = astroplant_capnp.RpcError.new_message(other=None)
                response.error = error
        elif which == "peripheralCommandLock":
            response.peripheralCommandLock = await rpc.peripheral_command_lock(
                request.peripheralCommandLock.peripheral
            )
        else:
            logger.warn(f"Received unknown kit RPC request: {which}")
            error = astroplant_capnp.RpcError.new_message(methodNotFound=None)
            response.error = error

        self._send_response(response)

    async def _on_request(self, data: bytes) -> None:
        if not self._handler:
            logger.warn("Received RPC request, but kit RPC handler is not registered.")
            return

        try:
            request = astroplant_capnp.KitRpcRequest.from_bytes_packed(data)
        except:
            logger.warn("received malformed RPC request")
            return

        await self._handle_request(request)
