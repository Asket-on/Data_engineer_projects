import json
from typing import Dict

import redis


class RedisClient:
    def __init__(self, host: str, port: int, password: str, cert_path: str) -> None:
        if host.endswith('.yandexcloud.net'):
            self._client = redis.StrictRedis(
                host=host,
                port=port,
                password=password,
                ssl=True,
                ssl_ca_certs=cert_path)
        else:
            self._client = redis.StrictRedis(
                host=host,
                port=port,
                password=password,
                ssl=False)

    def set(self, k, v):
        self._client.set(k, json.dumps(v))

    def get(self, k) -> Dict:
        obj: str = self._client.get(k)  # type: ignore
        return json.loads(obj)
