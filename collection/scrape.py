from functools import lru_cache, reduce
import itertools
import operator
import os
import pathlib
import time
from urllib.parse import parse_qsl, urlparse, ParseResult
import threading
from typing import Callable, Optional, Union, TypeVar

import requests

S = TypeVar("S", str, bytes)
AnyURL = Union[str, ParseResult]


class URLCache:
    def __init__(self, cache_dir: str, binary: bool = True, ttl: Optional[float] = float(5 * 24 * 60 * 60)):
        self.cache_dir = pathlib.Path(cache_dir)
        self.binary = binary
        self.ttl = ttl
        self.lock = threading.RLock()
        self._key_path = lru_cache(1000)(self._key_path)

    def _key_path(self, item: Union[str, ParseResult]) -> pathlib.Path:
        if not isinstance(item, ParseResult):
            item = urlparse(item)

        dir_ = reduce(
            operator.truediv, item.path.split('/'), self.cache_dir / item.netloc,
        )
        if item.query:
            name = '&'.join(
                itertools.starmap(
                    '{}={}'.format,
                    sorted(
                        parse_qsl(item.query),
                        key=operator.itemgetter(0)
                    )
                )
            )
        else:
            name = "?"
        return dir_ / name

    def _contains_path(self, path: pathlib.Path):
        if self.ttl is None:
            return path.exists()
        else:
            now = time.time()
            with self.lock:
                # lock to prevent error from file deletion before the right side of the `and`
                return path.exists() and path.stat().st_mtime > (now - self.ttl)

    def __contains__(self, key: AnyURL):
        path = self._key_path(key)
        return self._contains_path(path)

    def __delitem__(self, key: AnyURL):
        path = self._key_path(key)
        with self.lock:
            if self._contains_path(path):
                os.remove(str(path))
            else:
                raise KeyError(key)

    def __setitem__(self, key: AnyURL, value: S):
        path = self._key_path(key)
        with self.lock:
            if not path.parent.exists():
                path.parent.mkdir(parents=True)
            with open(str(path), 'wb' if self.binary else 'w') as f:
                f.write(value)

    def __getitem__(self, key: AnyURL) -> S:
        path = self._key_path(key)
        with self.lock:
            if self._contains_path(path):
                with open(str(path), 'rb' if self.binary else 'r') as f:
                    return f.read()
            else:
                raise KeyError(key)

    def get(self, key: AnyURL, default=None) -> Optional[S]:
        path = self._key_path(key)
        with self.lock:
            if self._contains_path(path):
                with open(str(path), 'rb' if self.binary else 'r') as f:
                    return f.read()
            else:
                return default

    def __call__(self, get: Callable[[AnyURL], S]):
        def get_(url: str):
            url_ = urlparse(url)
            result = self.get(url_)
            if result is None:
                s = get(url)
                self[url_] = s
                return s
            else:
                return result

        return get_


@URLCache(pathlib.Path(__file__).parent.parent / ".cache", binary=True)
def fetch_content(url: str) -> bytes:
    return requests.get(url, verify=False).content
