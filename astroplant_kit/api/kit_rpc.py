import logging
import abc
import asyncio
from .schema import astroplant_capnp


logger = logging.getLogger("astroplant_kit.api")


class KitRpcHandler(abc.ABC):
    """
    Abstract class representing the kit RPC handler interface.
    """
    @abc.abstractmethod
    async def version() -> str:
        pass

    @abc.abstractmethod
    async def uptime() -> int:
        pass


class KitRpc(object):
    """
    Handles MQTT messages to implement the kit RPC system.
    """
    def __init__(self, kit_rpc_response_handle):
        self._kit_rpc_response_handle = kit_rpc_response_handle
        self._handler = None

    def _register_handler(self, kit_rpc_handler):
        self._handler = kit_rpc_handler

    def _send_response(self, response):
        """
        Send an RPC response over MQTT.
        """
        self._kit_rpc_response_handle(response.to_bytes_packed())

    async def _handle_request(self, request):
        rpc = self._handler

        response = astroplant_capnp.KitRpcResponse.new_message(id = request.id)

        which = request.which()
        if which == 'version':
            response.version = await rpc.version()
        elif which == 'uptime':
            response.uptime = await rpc.uptime()
        else:
            logger.warn(f'Received unknown kit RPC request: {which}')
            error = astroplant_capnp.RpcError.new_message(methodNotFound = None)
            response.error = error

        self._send_response(response)

    async def _on_request(self, data):
        if not self._handler:
            logger.warn("Received RPC request, but kit RPC handler is not registered.")
            return

        try:
            request = astroplant_capnp.KitRpcRequest.from_bytes_packed(data)
        except:
            logger.warn("received malformed RPC request")
            return

        await self._handle_request(request)
