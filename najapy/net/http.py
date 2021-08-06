import os
import ssl
from contextlib import contextmanager

import aiohttp

from najapy.common.async_base import AsyncCirculatorForSecond, Utils

DEFAULT_TIMEOUT = aiohttp.client.ClientTimeout(total=60, connect=10, sock_read=60, sock_connect=10)
DOWNLOAD_TIMEOUT = aiohttp.client.ClientTimeout(total=600, connect=10, sock_read=600, sock_connect=10)

CACERT_FILE = os.path.join(
    os.path.split(os.path.abspath(__file__))[0],
    r'../static/cacert.pem'
)


class _AsyncCirculator(AsyncCirculatorForSecond):

    async def _sleep(self):
        await Utils.sleep(self._current)


def _json_decoder(val, **kwargs):
    try:
        return Utils.json_decode(val, **kwargs)
    except Exception as err:
        Utils.log.warning(f'http client json decode error: {err} => {val}')


@contextmanager
def _catch_error():
    """异常捕获

    通过with语句捕获异常，代码更清晰，还可以使用Ignore异常安全的跳出with代码块

    """

    try:

        yield

    except aiohttp.ClientResponseError as err:

        if err.status < 500:
            Utils.log.warning(err)
        else:
            Utils.log.error(err)

    except Exception as err:

        Utils.log.error(err)


class Result(dict):

    def __init__(self, status, headers, body):
        super().__init__(status=status, headers=headers, body=body)

    def __bool__(self):
        return (self.status >= 200) and (self.status <= 299)

    @property
    def status(self):
        return self.get(r'status')

    @property
    def headers(self):
        return self.get(r'headers')

    @property
    def body(self):
        return self.get(r'body')


class _HTTPClient:
    """HTTP客户端基类
    """

    def __init__(self, retry_count=5, timeout=None, raise_for_status=True, **kwargs):

        global DEFAULT_TIMEOUT

        self._ssl_context = self._create_ssl_context()

        self._retry_count = retry_count

        self._session_config = kwargs
        self._session_config[r'timeout'] = timeout if timeout is not None else DEFAULT_TIMEOUT
        self._session_config.setdefault(r'raise_for_status', raise_for_status)

    async def _handle_response(self, response):

        return await response.read()

    def _create_ssl_context(self):

        global CACERT_FILE

        return ssl.create_default_context(cafile=CACERT_FILE)

    async def send_request(
            self, method, url, data=None, params=None, cookies=None, headers=None, **settings
    ):

        response = None

        if headers is None:
            headers = {}

        settings[r'data'] = data
        settings[r'params'] = params
        settings[r'cookies'] = cookies
        settings[r'headers'] = headers

        Utils.log.debug(
            r'{0} {1} => {2}'.format(
                method,
                url,
                str({key: val for key, val in settings.items() if isinstance(val, (str, list, dict))})
            )
        )

        settings.setdefault(r'ssl', self._ssl_context)

        async for times in _AsyncCirculator(max_times=self._retry_count):

            try:

                async with aiohttp.ClientSession(**self._session_config) as _session:

                    async with _session.request(method, url, **settings) as _response:
                        response = Result(
                            _response.status,
                            dict(_response.headers),
                            await self._handle_response(_response)
                        )

            except aiohttp.ClientResponseError as err:

                # 重新尝试的话，会记录异常，否则会继续抛出异常

                if err.status < 500:
                    raise err
                elif times >= self._retry_count:
                    raise err
                else:
                    Utils.log.warning(err)
                    continue

            except aiohttp.ClientError as err:

                if times >= self._retry_count:
                    raise err
                else:
                    Utils.log.warning(err)
                    continue

            except Exception as err:

                raise err

            else:

                Utils.log.info(f'{method} {url} => status:{response.status}')
                break

            finally:

                if times > 1:
                    Utils.log.warning(f'{method} {url} => retry:{times}')

        return response


class _HTTPJsonMixin:
    """Json模式混入类
    """

    async def _handle_response(self, response):
        return await response.json(encoding=r'utf-8', loads=_json_decoder, content_type=None)


class HTTPClient(_HTTPClient):
    """HTTP客户端，普通模式
    """

    async def get(self, url, params=None, *, cookies=None, headers=None):
        result = None

        with _catch_error():
            resp = await self.send_request(aiohttp.hdrs.METH_GET, url, None, params, cookies=cookies, headers=headers)

            result = resp.body

        return result

    async def options(self, url, params=None, *, cookies=None, headers=None):
        result = None

        with _catch_error():
            resp = await self.send_request(aiohttp.hdrs.METH_OPTIONS, url, None, params, cookies=cookies,
                                           headers=headers)

            result = resp.headers

        return result

    async def head(self, url, params=None, *, cookies=None, headers=None):
        result = None

        with _catch_error():
            resp = await self.send_request(aiohttp.hdrs.METH_HEAD, url, None, params, cookies=cookies, headers=headers)

            result = resp.headers

        return result

    async def post(self, url, data=None, params=None, *, cookies=None, headers=None):
        result = None

        with _catch_error():
            resp = await self.send_request(aiohttp.hdrs.METH_POST, url, data, params, cookies=cookies, headers=headers)

            result = resp.body

        return result

    async def put(self, url, data=None, params=None, *, cookies=None, headers=None):
        result = None

        with _catch_error():
            resp = await self.send_request(aiohttp.hdrs.METH_PUT, url, data, params, cookies=cookies, headers=headers)

            result = resp.body

        return result

    async def patch(self, url, data=None, params=None, *, cookies=None, headers=None):
        result = None

        with _catch_error():
            resp = await self.send_request(aiohttp.hdrs.METH_PATCH, url, data, params, cookies=cookies, headers=headers)

            result = resp.body

        return result

    async def delete(self, url, params=None, *, cookies=None, headers=None):
        result = None

        with _catch_error():
            resp = await self.send_request(aiohttp.hdrs.METH_DELETE, url, None, params, cookies=cookies,
                                           headers=headers)

            result = resp.body

        return result


class HTTPJsonClient(_HTTPJsonMixin, HTTPClient):
    """HTTP客户端，Json模式
    """
    pass


class HTTPClientPool(HTTPClient):
    """HTTP带连接池客户端，普通模式
    """

    def __init__(self,
                 retry_count=5, use_dns_cache=True, ttl_dns_cache=10,
                 limit=100, limit_per_host=0, timeout=None, raise_for_status=True,
                 **kwargs
                 ):
        super().__init__(retry_count, timeout, raise_for_status, **kwargs)

        self._tcp_connector = aiohttp.TCPConnector(
            use_dns_cache=use_dns_cache,
            ttl_dns_cache=ttl_dns_cache,
            ssl=self._ssl_context,
            limit=limit,
            limit_per_host=limit_per_host,
        )

        self._session_config[r'connector'] = self._tcp_connector
        self._session_config[r'connector_owner'] = False

    async def close(self):
        if not self._tcp_connector.closed:
            await self._tcp_connector.close()


class HTTPJsonClientPool(_HTTPJsonMixin, HTTPClientPool):
    """HTTP带连接池客户端，Json模式
    """
    pass
