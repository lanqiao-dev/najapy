from typing import List, Tuple, Union

from starlette.websockets import WebSocket

from najapy.common.buffer import QueueBuffer
from najapy.common.metaclass import Singleton


class WSConnectionManager(Singleton):
    def __init__(self):
        super().__init__()
        self._active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self._active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self._active_connections.remove(websocket)

    async def send_personal_message(self, message: str, websocket: WebSocket):
        await websocket.send_text(message)

    async def broadcast(self, message: str):
        for connection in self._active_connections:
            await connection.send_text(message)


class WSConnectionManagerWithBuffer(WSConnectionManager):
    def __init__(self, buffer_size_limit=0xffff, buffer_timeout=1):
        super().__init__()

        self._buffer = QueueBuffer(self._handle_data, buffer_size_limit, buffer_timeout)

    async def _handle_data(self, data: Union[Tuple[WebSocket, str], str]):
        if isinstance(data, tuple):
            websocket, msg = data
            await websocket.send_text(msg)

        if isinstance(data, str):
            for connection in self._active_connections:
                await connection.send_text(data)

    def send_message(self, data: Union[Tuple[WebSocket, str], str]):
        self._buffer.append(data)
