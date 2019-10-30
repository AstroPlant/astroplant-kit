import logging
import asyncio
import datetime
import json
from .schema import astroplant_capnp
from .errors import *

logger = logging.getLogger("astroplant_kit.api.server_rpc")


class ServerRpc(object):
    """
    Handles MQTT messages to implement the server RPC system.
    """
    def __init__(self, event_loop, server_rpc_request_handle):
        self._event_loop = event_loop
        self._request_handle = server_rpc_request_handle

        self._rpc_next_request_id = 0
        self._rpc_response_queue = {}
        self._rpc_response_timeout = []
        self._event_loop.create_task(self._cleanup_rpc_response_queue())

    def _on_response(self, data):
        """
        Handles payloads received on `kit/{kit_serial}/server-rpc/response`.
        """
        response = astroplant_capnp.ServerRpcResponse.from_bytes_packed(data)
        id = response.id
        if id in self._rpc_response_queue:
            (value, exception) = self._rpc_response_queue[id]
            del self._rpc_response_queue[id]
            if response.which() == 'error':
                error = response.error
                error_which = error.which()
                if error_which == 'other':
                    exception(RpcErrorOther())
                elif error_which == 'methodNotFound':
                    exception(RpcErrorMethodNotFound())
                elif error_which == 'rateLimit':
                    exception(RpcErrorRateLimit(error.rateLimit))
                else:
                    exception(RpcErrorUnknown())
                return
            value(response)

    def _send_request(self, request):
        """
        Send an RPC request over MQTT.
        """
        self._request_handle(request.to_bytes_packed())

    def _next_base_request(self):
        """
        Prepares a new RPC request, and creates a future to be called when the
        response is received.

        Should only be called from within a running event loop.
        """
        loop = asyncio.get_event_loop()
        fut = loop.create_future()
        request_id = self._rpc_next_request_id
        self._rpc_next_request_id += 1

        value_callback = lambda value: loop.call_soon_threadsafe(fut.set_result, value)
        exception_callback = lambda value: loop.call_soon_threadsafe(fut.set_exception, value)
        self._rpc_response_queue[request_id] = (value_callback, exception_callback)
        self._rpc_response_timeout.append((request_id, datetime.datetime.now()))

        return (astroplant_capnp.ServerRpcRequest.new_message(id = request_id), fut)

    async def _cleanup_rpc_response_queue(self):
        while True:
            await asyncio.sleep(10)
            now = datetime.datetime.now()
            while len(self._rpc_response_timeout) > 0:
                (id, start) = self._rpc_response_timeout[0]
                self._rpc_response_timeout.pop(0)
                if (now - start).total_seconds() >= 2:
                    if id in self._rpc_response_queue:
                        (value, exception) = self._rpc_response_queue[id]
                        del self._rpc_response_queue[id]
                        exception(ServerRpcRequestTimedOut())
                    logger.debug("running server RPC response handler cleanup")

    async def version(self):
        """
        Request the version of the RPC server.
        """
        (request, fut) = self._next_base_request()
        request.version = None
        self._send_request(request)

        response = await fut
        if response.which() == 'version':
            return response.version
        else:
            raise RpcInvalidResponse()

    async def get_active_configuration(self):
        """
        Request the active configuration of this kit.
        """
        (request, fut) = self._next_base_request()
        request.getActiveConfiguration = None
        self._send_request(request)

        response = await fut
        print(response)
        if response.which() == 'getActiveConfiguration':
            maybe_configuration = response.getActiveConfiguration
            if maybe_configuration.which() == 'configuration':
                return json.loads(maybe_configuration.configuration)
            else:
                return None
        else:
            raise RpcInvalidResponse()

    async def get_quantity_types(self):
        """
        Request the quantity types known to the RPC server.
        """
        (request, fut) = self._next_base_request()
        request.getQuantityTypes = None
        self._send_request(request)

        response = await fut
        print(response)
        if response.which() == 'getQuantityTypes':
            return json.loads(response.getQuantityTypes)
        else:
            raise RpcInvalidResponse()
