import functools
from random import random
import time

class DelayWithExponential:
    def __init__(
        self, base=0.1, exponent=2, jitter=True, max_delay=2 * 3600
    ):
        self._base = base
        self._exponent = exponent
        self._jitter = jitter
        self._max_delay = max_delay

    def __call__(self, attempt):
        delay = float(self._base) * (self._exponent ** attempt)
        if self._jitter:
            delay *= random()
        delay = min(float(self._max_delay), delay)
        return delay

class RetryWithExponentialBackoff(object):
    def __init__(
        self, base=0.1, exponent=2, jitter=True, max_delay=2 * 3600  # 100ms  # 2 hours
    ):
        self._get_delay = DelayWithExponential(base, exponent, jitter, max_delay)

    def retry(self, attempt):
        delay = self._get_delay(attempt)
        time.sleep(delay)

def retry_with(handle_retry, exceptions, conditions, max_attempts):
    def wrapper(func):
        @functools.wraps(func)
        def decorated(*args, **kwargs):
            error = None
            result = None
            for attempt in range(1, max_attempts + 1):
                try:
                    result = func(*args, **kwargs)
                    if any(guard(result) for guard in conditions):
                        handle_retry.retry(func, args, kwargs, None, attempt)
                        continue
                    return result
                except Exception as err:
                    error = err
                    if any(isinstance(err, exc) for exc in exceptions):
                        handle_retry.retry(func, args, kwargs, err, attempt)
                        continue
                    break
            if error is not None:
                raise error
            return result

        return decorated

    return wrapper
