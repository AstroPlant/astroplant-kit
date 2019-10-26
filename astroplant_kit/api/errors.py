class RpcError(Exception):
    pass


class ServerRpcRequestTimedOut(RpcError):
    pass


class RpcErrorOther(RpcError):
    pass


class RpcErrorUnknown(RpcError):
    pass


class RpcErrorMethodNotFound(RpcError):
    pass


class RpcErrorRateLimit(RpcError):
    pass


class RpcInvalidResponse(RpcError):
    pass
