import random
from typing import Optional, Final

from core.cluster import ClusterManager
from core.message import JsonMessage
from core.network import TcpClient, ConnectionStub
from core.server import ServerInfo, Server
from craq.craq_server import CraqServer

START_PORT: Final[int] = 9900
POOL_SZ: Final[int] = 32

class CraqClient:
    def __init__(self, infos: list[ServerInfo]):
        self.conns: list[TcpClient] = [TcpClient(info) for info in infos]

    def set(self, key: str, val: str) -> bool:
        response = self.conns[0].send(JsonMessage({
            "type": "SET",
            "key": key,
            "val": val
        }))
        print(response)
        assert response is not None
        return response["status"] == "OK"

    def get(self, key: str) -> tuple[bool, Optional[str]]:
        # Randomly choose a server for read operations
        response: Optional[JsonMessage] = self._get_random_server().send(JsonMessage({"type": "GET", "key": key}))
        print(response)
        # if response is None:
        #     return False, "No response from server"
        # if response["status"] == "OK":
        #     return True, response["val"]
        # return False, response["status"]
        # server = random.choice(self.conns)
        # response = server.send(JsonMessage({
        #     "type": "GET",
        #     "key": key
        # }))
        assert response is not None
        if response["status"] == "OK":
            return True, response["val"]
        return False, response["status"]
    
    def _get_random_server(self) -> TcpClient:
        # Randomly select a server from the list of connections
        return random.choice(self.conns)
class CraqCluster(ClusterManager):
    def __init__(self) -> None:
        self.a = ServerInfo("a", "localhost", 9900)
        self.b = ServerInfo("b", "localhost", 9901)
        self.c = ServerInfo("c", "localhost", 9902)
        self.d = ServerInfo("d", "localhost", 9903)

        self.prev = {self.a: None, self.b: self.a, self.c: self.b, self.d: self.c}
        self.next = {self.a: self.b, self.b: self.c, self.c: self.d, self.d: None}

        super().__init__(
            master_name="d",
            topology={self.a: {self.b, self.d}, self.b: {self.c,self.d}, self.c: {self.d}, self.d: set()},
            sock_pool_size=POOL_SZ,
        )

    def connect(self) -> CraqClient:
        return CraqClient([self.a, self.b, self.c, self.d])

    def create_server(self, si: ServerInfo, connection_stub: ConnectionStub) -> Server:
        return CraqServer(info=si, connection_stub=connection_stub,
                          next=self.next[si], prev=self.prev[si], tail=self.d)