@0xfab5802082af6f74;

struct RawMeasurement {
  kitSerial @0 :Text;
  datetime @1 :UInt64;
  peripheral @2 :Int32;
  physicalQuantity @3 :Text;
  physicalUnit @4 :Text;
  value @5 :Float64;
}

struct AggregateMeasurement {
  kitSerial @0 :Text;
  datetimeStart @1 :UInt64;
  datetimeEnd @2 :UInt64;
  peripheral @3 :Int32;
  physicalQuantity @4 :Text;
  physicalUnit @5 :Text;
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
    getActiveConfiguration @2 :Void;
  }
}

struct ServerRpcResponse {
  id @0 :UInt64;

  union {
    error @1 :RpcError;
    version @2 :Text;
    getActiveConfiguration @3 :ActiveConfiguration;
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
