import time
import threading


class RateLimiter:
    """
    Token bucket rate limiter.
    Gemini free tier: 15 requests/minute.
    We stay conservative at 10 requests/minute.
    """

    def __init__(self, max_calls: int = 10, period: int = 60):
        self._max_calls  = max_calls
        self._period     = period
        self._calls      = []
        self._lock       = threading.Lock()

    def wait_if_needed(self):
        """
        Block until a request can be made within rate limits.
        Called before every Gemini API call.
        """
        with self._lock:
            now = time.time()

            # Remove calls older than the period window
            self._calls = [t for t in self._calls if now - t < self._period]

            if len(self._calls) >= self._max_calls:
                # Wait until oldest call falls outside the window
                wait_time = self._period - (now - self._calls[0])
                if wait_time > 0:
                    print(f"  Rate limit reached — waiting {wait_time:.1f}s...")
                    time.sleep(wait_time)
                self._calls = [t for t in self._calls
                               if time.time() - t < self._period]

            self._calls.append(time.time())

    def requests_remaining(self) -> int:
        """How many requests can still be made in the current window."""
        now = time.time()
        with self._lock:
            recent = [t for t in self._calls if now - t < self._period]
            return max(0, self._max_calls - len(recent))


# Single shared instance
rate_limiter = RateLimiter(max_calls=10, period=60)