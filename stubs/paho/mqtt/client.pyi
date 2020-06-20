from typing import Optional, Tuple, Dict, Callable, Generic


class MQTTMessage:
    timestamp: int
    topic: str
    payload: bytes
    qos: int
    retain: bool
    mid: int


class MQTTMessageInfo:
    mid: int
    rc: int

    def wait_for_publish(self) -> None:
        ...

    def is_published(self) -> bool:
        ...


class Client(Generic[T]):
    on_connect: Callable[[Client, T, Dict[str, int], int]]
    on_disconnect: Callable[[Client, T, int]]
    on_publish: Callable[[Client, T, int]]
    on_message: Callable[[Client, T, MQTTMessage]]

    def publish(
        self,
        topic: str,
        payload: Optional[bytes] = None,
        qos: int = 0,
        retain: bool = False,
    ) -> MQTTMessageInfo:
        ...

    def username_pw_set(self, username: str, password: Optional[str] = None) -> None:
        ...

    def user_data_set(self, userdata: T) -> None:
        ...

    def will_set(
        self,
        topic: str,
        payload: Optional[bytes] = None,
        qos: int = 0,
        retain: bool = False,
    ) -> None:
        ...

    def reconnect_delay_set(self, min_delay: int = 1, max_delay: int = 128) -> None:
        ...

    def connect_async(
        self, host: str, port: int = 1883, keepalive: int = 60, bind_address: str = ""
    ) -> None:
        ...

    def connect_srv(
        self, domain: str, keepalive: int = 60, bind_address: str = ""
    ) -> None:
        ...

    def subscribe(self, topic: str, qos: int = 0) -> Tuple[int, int]:
        ...

    def unsubscribe(self, topic: str) -> Tuple[int, int]:
        ...

    def loop_start(self) -> None:
        ...

    def loop_stop(self) -> None:
        ...
