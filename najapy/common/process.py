from multiprocessing.shared_memory import SharedMemory

from najapy.common.async_base import Utils
from najapy.common.base import ContextManager
from najapy.common.struct import ByteArrayAbstract


class SharedByteArray(ByteArrayAbstract, ContextManager):

    def __init__(self, name=None, create=False, size=0):

        ByteArrayAbstract.__init__(self)

        self._shared_memory = SharedMemory(name, create, size)
        self._create_mode = create

    def _context_release(self):

        self.release()

    def release(self):

        self._shared_memory.close()

        if self._create_mode:
            self._shared_memory.unlink()

    def read(self, size):

        return self._shared_memory.buf[:size]

    def write(self, buffer):

        self._shared_memory.buf[:len(buffer)] = buffer


class HeartbeatChecker(ContextManager):

    def __init__(self, name=r'default', timeout=60):
        self._name = f'heartbeat_{name}'

        self._timeout = timeout

        try:
            self._byte_array = SharedByteArray(self._name, True, 8)
        except Exception as _:
            self._byte_array = SharedByteArray(self._name)

    def _context_release(self):
        self.release()

    @property
    def refresh_time(self):
        if self._byte_array is not None:
            return self._byte_array.read_unsigned_int()
        else:
            return 0

    def release(self):

        if self._byte_array is not None:
            self._byte_array.release()
            self._byte_array = None

    def check(self):
        return (Utils.timestamp() - self.refresh_time) < self._timeout

    def refresh(self):
        if self._byte_array is not None:
            self._byte_array.write_unsigned_int(Utils.timestamp())