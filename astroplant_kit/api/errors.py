class RpcError(Exception):
    """
    RPC base error.
    """

    def __init__(self, message=None):
        if message is not None:
            super().__init__(f"rpc error: {message}")
        else:
            super().__init__("rpc error")


class ServerRpcRequestTimedOut(RpcError):
    """
    The RPC server did not respond in time.
    """

    def __init__(self):
        super().__init__("server request timed out")


class RpcErrorOther(RpcError):
    """
    An other RPC error occurred.
    """

    def __init__(self):
        super().__init__("other")


class RpcErrorUnknown(RpcError):
    """
    An unknown RPC error occurred.
    """

    def __init__(self):
        super().__init__("unknown")


class RpcErrorMethodNotFound(RpcError):
    """
    The RPC method was not found.
    """

    def __init__(self):
        super().__init__("method not found")


class RpcErrorRateLimit(RpcError):
    """
    The RPC request was rate limited.
    """

    def __init__(self):
        super().__init__("rate limited")


class RpcInvalidResponse(RpcError):
    """
    The RPC response was invalid.
    """

    def __init__(self):
        super().__init__("invalid response")
