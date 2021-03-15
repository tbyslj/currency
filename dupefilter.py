"""
@File: logger
@Time: 2020-07-01 14:37:16
@Auth: tang
"""
import hashlib
import weakref

from gli.connect.redisdb import RedisDB
from w3lib.url import canonicalize_url

_fingerprint_cache = weakref.WeakKeyDictionary()

class Dupefilter(object):
    def __init__(self):
        self.server = RedisDB()
        self.key = "distributed"

    def request_fingerprint(self, request, include_headers=None, keep_fragments=False):
        if include_headers:
            include_headers = tuple(self.to_bytes(h.lower()) for h in sorted(include_headers))
        cache = _fingerprint_cache.setdefault(request, {})
        cache_key = (include_headers, keep_fragments)
        if cache_key not in cache:
            fp = hashlib.sha1()
            fp.update(self.to_bytes(request.method))
            fp.update(self.to_bytes(canonicalize_url(request.url, keep_fragments=keep_fragments)))
            fp.update(request.body or b'')
            if include_headers:
                for hdr in include_headers:
                    if hdr in request.headers:
                        fp.update(hdr)
                        for v in request.headers.getlist(hdr):
                            fp.update(v)
            cache[cache_key] = fp.hexdigest()
        return cache[cache_key]

    def to_bytes(self, text, encoding=None, errors='strict'):
        """Return the binary representation of ``text``. If ``text``
        is already a bytes object, return it as-is."""
        if isinstance(text, bytes):
            return text
        if not isinstance(text, str):
            raise TypeError('to_bytes must receive a str or bytes '
                            f'object, got {type(text).__name__}')
        if encoding is None:
            encoding = 'utf-8'
        return text.encode(encoding, errors)

    def request_seen(self, request):
        fp = self.request_fingerprint(request)
        # This returns the number of values added, zero if already exists.
        added = self.server.addset(self.key, fp)
        return added
