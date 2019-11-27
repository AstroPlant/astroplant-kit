@0xfab5802082af6f74;

struct RawMeasurement {
  id @0 :Data;
  kitSerial @1 :Text;
  datetime @2 :UInt64;
  peripheral @3 :Int32;
  quantityType @4 :Int32;
  value @5 :Float64;
}

struct AggregateMeasurement {
  id @0 :Data;
  kitSerial @1 :Text;
  datetimeStart @2 :UInt64;
  datetimeEnd @3 :UInt64;
  peripheral @4 :Int32;
  quantityType @5 :Int32;
  aggregateType @6 :Text;
  value @7 :Float64;
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
    getQuantityTypes @2 :Void;
    getActiveConfiguration @3 :Void;
  }
}

struct ServerRpcResponse {
  id @0 :UInt64;

  union {
    error @1 :RpcError;
    version @2 :Text;
    getQuantityTypes @3 :Text;
    getActiveConfiguration @4 :ActiveConfiguration;
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
