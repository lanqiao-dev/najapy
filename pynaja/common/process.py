import os
from multiprocessing.shared_memory import SharedMemory

from pynaja.common.async_base import Utils
from pynaja.common.base import ContextManager
from pynaja.common.struct import ByteArrayAbstract


class SharedByteArray(SharedMemory, ByteArrayAbstract, ContextManager):

    def __init__(self, name=None, create=False, size=0):

        SharedMemory.__init__(self, name, create, size)
        ByteArrayAbstract.__init__(self)

    def _context_release(self):

        self.release()

    def release(self):

        self.close()

        if (self._flags & os.O_CREAT) != 0:
            self.unlink()

    def read(self, size):

        return self._buf[:size]

    def write(self, buffer):

        self._buf[:len(buffer)] = buffer


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
        return True
        # return (Utils.timestamp() - self.refresh_time) < self._timeout

    def refresh(self):

        if self._byte_array is not None:
            self._byte_array.write_unsigned_int(Utils.timestamp())
