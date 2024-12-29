import json
from enum import Enum
from typing import Optional, Final, Dict, List

from core.logger import server_logger
from core.message import JsonMessage, JsonMessage
from core.network import ConnectionStub
from core.server import Server, ServerInfo


class RequestType(Enum):
  SET = 1
  GET = 2

class KVGetRequest:
  def __init__(self, msg: JsonMessage):
    self._json_message = msg
    assert "key" in self._json_message, self._json_message

  @property
  def key(self) -> str:
    return self._json_message["key"]

  @property
  def json_msg(self) -> JsonMessage:
    return self._json_message


class KVSetRequest:
  def __init__(self, msg: JsonMessage):
    self._json_message = msg
    assert "key" in self._json_message, self._json_message
    assert "val" in self._json_message, self._json_message

  @property
  def key(self) -> str:
    return self._json_message["key"]

  @property
  def val(self) -> str:
    return self._json_message["val"]

  @property
  def version(self) -> Optional[int]:
    return self._json_message.get("ver")

  @version.setter
  def version(self, ver: int) -> None:
    self._json_message['ver'] = ver

  @property
  def json_msg(self) -> JsonMessage:
    return self._json_message

  def __str__(self) -> str:
    return str(self._json_message)


class CraqServer(Server):
  """Chain replication. GET is only served by tail"""

  def __init__(self, info: ServerInfo, connection_stub: ConnectionStub,
               next: Optional[ServerInfo], prev: Optional[ServerInfo],
               tail: ServerInfo) -> None:
    super().__init__(info, connection_stub)
    self.next: Final[Optional[str]] = None if next is None else next.name
    self.prev: Final[Optional[str]] = prev if prev is None else prev.name
    self.tail: Final[str] = tail.name
    self.versions: Dict[str, List[Dict]] = {}  # Key -> List of versions
    self.d: dict[str, str] = {} # Key-Value store

  def _process_req(self, msg: JsonMessage) -> JsonMessage:
    if msg.get("type") == RequestType.GET.name:
      return self._get(KVGetRequest(msg))
    elif msg.get("type") == RequestType.SET.name:
      return self._set(KVSetRequest(msg))
    elif msg.get("type") == "VERSION":
      return self._version(msg)
    else:
      server_logger.critical("Invalid message type")
      return JsonMessage({"status": "Unexpected type"})

  def _get(self, req: KVGetRequest) -> JsonMessage:
    key = req.key
    if key not in self.versions or not self.versions[key]:
      return JsonMessage({"status": "NOT_FOUND"})
    
    latest_version = self.versions[key][-1]
    if latest_version["clean"]:
        return JsonMessage({"status": "OK", "val": latest_version["val"]})
    else:
      tail_response = self._connection_stub.send(from_=self._info.name, to=self.tail,
                                                 message=JsonMessage({"type": "VERSION", "key": key}))
      if tail_response is None:
        return JsonMessage({"status": "ERROR", "message": "Failed to contact tail"})
      version = tail_response.get("ver")
      for v in self.versions[key]:
        if v["ver"] == version:
          return JsonMessage({"status": "OK", "val": v["val"]})
      return JsonMessage({"status": "ERROR", "message": "Version not found"})  

  def _set(self, req: KVSetRequest) -> JsonMessage:
    key = req.key
    val = req.val
    
    version = len(self.versions.get(key, [])) + 1

    if req.version is not None:
        version = req.version
    elif  key in self.versions:
        version = self.versions[key][-1]["ver"] + 1
    else:
        version = 1
    new_version = {"val": val, "ver": version, "clean": False}
    if self.next is not None:
        if key not in self.versions:
            self.versions[key] = []
        self.versions[key].append(new_version)
    else:
       if key not in self.versions:
            self.versions[key] = []
            self.versions[key].append(new_version)
    if self.next is not None:
        req.version = version
        response = self._connection_stub.send(from_=self._info.name, to=self.next,
                                 message=req.json_msg)
        if response is None:
            return JsonMessage({"status": "ERROR", "message": "Failed to contact next"})
        ver = response["ver"]
        if response["status"] == "OK":
            for v in self.versions[key]:
                if v["ver"] == ver:
                    v["clean"] = True
                    break
        # delete older versions
        temp = self.versions[key]
        self.versions[key] = []
        for v in temp:
            if v["ver"] >= ver:
                self.versions[key].append(v)
        
        return JsonMessage({"status": "OK", "ver": ver})
    else:
        new_version["clean"] = True
        self.versions[key][-1] = new_version
        return JsonMessage({"status": "OK", "ver": version})

  def _version(self, req: JsonMessage) -> JsonMessage:
    key = req["key"]
    if key not in self.versions or not self.versions[key]:
        return JsonMessage({"status": "NOT_FOUND"})
    return JsonMessage({"status": "OK", "ver": self.versions[key][-1]["ver"]})