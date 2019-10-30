@0xfab5802082af6f74;

struct RawMeasurement {
  kitSerial @0 :Text;
  datetime @1 :UInt64;
  peripheral @2 :Int32;
  quantityType @3 :Int32;
  value @4 :Float64;
}

struct AggregateMeasurement {
  kitSerial @0 :Text;
  datetimeStart @1 :UInt64;
  datetimeEnd @2 :UInt64;
  peripheral @3 :Int32;
  quantityType @4 :Int32;
  aggregateType @5 :Text;
  value @6 :Float64;
}

struct RpcError {
  union {
    other @0 :Void;
    methodNotFound @1 :Void;
    rateLimit @2 :UInt64;
  }
}

struct ServerRpcRequest {
  id @0 :UInt64;

  union {
    version @1 :Void;
    getActiveConfiguration @2 :Void;
    getQuantityTypes @3 :Void;
  }
}

struct ServerRpcResponse {
  id @0 :UInt64;

  union {
    error @1 :RpcError;
    version @2 :Text;
    getActiveConfiguration @3 :ActiveConfiguration;
    getQuantityTypes @4 :Text;
  }
}

struct ActiveConfiguration {
  union {
    configuration @0 :Text;
    none @1 :Void;
  }
}

struct KitRpcRequest {
  id @0 :UInt64;

  union {
    version @1 :Void;
    uptime @2 :Void;
  }
}

struct KitRpcResponse {
  id @0 :UInt64;

  union {
    error @1 :RpcError;
    version @2 :Text;
    uptime @3 :UInt64;
  }
}
