import asyncio
import json
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


class HTTP:
    def __init__(self, workers=4):
        self.pool = ThreadPoolExecutor(max_workers=workers)

    async def request(self, url, headers=None, data=None, method=None):
        def fetch():
            request = urllib.request.Request(url, data=data, headers={'User-Agent': 'QuarrySourceScout/0.1', **(headers or {})}, method=method)
            try:
                response = urllib.request.build_opener(NoRedirect()).open(request, timeout=20)
            except urllib.error.HTTPError as error:
                response = error
            with response:
                body = response.read(4_000_001)
                if len(body) > 4_000_000:
                    raise ValueError('Response size exceeds limit')
                return response.status, dict(response.headers), body
        return await asyncio.get_running_loop().run_in_executor(self.pool, fetch)

    async def json(self, url, **kwargs):
        status, headers, body = await self.request(url, **kwargs)
        if status != 200:
            raise RuntimeError(f'HTTP {status}')
        return json.loads(body)

    def close(self):
        self.pool.shutdown(wait=False, cancel_futures=True)
