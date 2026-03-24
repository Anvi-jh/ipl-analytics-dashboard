import time
import hashlib
import threading
from config import INSIGHT_CACHE_TTL


class InsightCache:
    """
    Simple in-memory cache for LLM insights.
    Avoids redundant Gemini API calls on every dashboard refresh.

    TTL = 300 seconds (5 minutes) by default.
    Thread-safe for use with the scheduler.
    """

    def __init__(self, ttl: int = INSIGHT_CACHE_TTL):
        self._cache  = {}
        self._lock   = threading.Lock()
        self._ttl    = ttl
        self._hits   = 0
        self._misses = 0

    def _make_key(self, *args) -> str:
        """Stable cache key from any arguments."""
        raw = "|".join(str(a) for a in args)
        return hashlib.md5(raw.encode()).hexdigest()

    def get(self, *args) -> str | None:
        """Return cached value if it exists and hasn't expired."""
        key = self._make_key(*args)
        with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                self._misses += 1
                return None
            value, timestamp = entry
            if time.time() - timestamp > self._ttl:
                del self._cache[key]
                self._misses += 1
                return None
            self._hits += 1
            return value

    def set(self, value: str, *args):
        """Store a value in the cache."""
        key = self._make_key(*args)
        with self._lock:
            self._cache[key] = (value, time.time())

    def invalidate(self, *args):
        """Manually invalidate a specific cache entry."""
        key = self._make_key(*args)
        with self._lock:
            self._cache.pop(key, None)

    def clear(self):
        """Clear entire cache."""
        with self._lock:
            self._cache.clear()
            print("Cache cleared.")

    def stats(self) -> dict:
        """Return cache hit/miss stats."""
        total = self._hits + self._misses
        hit_rate = round(self._hits / total * 100, 1) if total > 0 else 0
        return {
            "hits":     self._hits,
            "misses":   self._misses,
            "hit_rate": f"{hit_rate}%",
            "size":     len(self._cache),
            "ttl":      self._ttl,
        }


# Single shared instance used across the whole app
insight_cache = InsightCache()