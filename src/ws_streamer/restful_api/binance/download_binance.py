#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
https://medium.com/@danielbriano

Binance Asynchronous OHLCV Downloader

This module provides a class to download OHLCV (Open, High, Low, Close, Volume) data
from the Binance API as efficiently as possible using asynchronous requests.

Key Features:
- Asynchronous data fetching for improved performance
- Rate limiting to comply with Binance API restrictions
- Circuit breaker pattern to handle and recover from API errors
- Adjustable request rates based on API usage feedback

Classes:
- BinanceClient: Main class for interacting with the Binance API
- RateLimiter: Manages and adjusts request rates
- CircuitBreaker: Implements the circuit breaker pattern for error handling
- RateLimitManager: Manages rate limit states and ban durations

Usage:
    client = BinanceClient()
    ohlcv_data = await client.get_ohlcv('BTCUSDT', '1h', start_time, end_time)

Note:
This module is designed to work within the constraints of the Binance API's rate limits.
Users should be aware of and respect these limits to avoid temporary IP bans.

Binance API Rate Limits:
- REQUEST_WEIGHT: 1200 per minute
- ORDERS: 50 per 10 seconds, 160000 per day
- RAW_REQUESTS: 6100 per 5 minutes

Created on Dec 17 06:36:20 2024

@author: dhaneor
"""
import asyncio
import logging
import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from threading import Lock
from time import time
from typing import Any, Dict, List, Tuple

import aiohttp

LOG_LEVEL = logging.INFO

# Increase this for increased speed at the beginning of a download/minute.
# 15 seems to be a good number which means that at the end of the minute,
# there is only a short waiting time for the reset of the API 1m limit.
# If the latency is low, it may even make sense to decrease this value.
MAX_WORKERS = 15  # max number of parallel requests to the Binance API
MAX_RETRIES = 5  # number of retries before giving up
# These are the default values that can also be overridden when initializing
# the BinanceClient class.

BASE_URL = "https://api.binance.com"
API_ENDPOINT = "/api/v3/klines"

KLINES_LIMIT = 1000  # how may klines to request in one API call
BASE_DELAY = MAX_WORKERS / 25  # base delay for rate limiting (in seconds)

RATE_LIMIT_BARRIER = 1200  # this is the Binance API rate limit (see above)
HARD_LIMIT = RATE_LIMIT_BARRIER - 50  # limit hard above this threshhold
SOFT_LIMIT = RATE_LIMIT_BARRIER * 0.6  # start limiting above this threshhold

RAW_REQUEST_LIMIT = 6100  # requests per 5 minutes (see above)
LONG_DELAY_THRESHHOLD = RAW_REQUEST_LIMIT * 0.9  # add even more delay above ...

SIMULATE_429_ERRORS = False  # activate this to test the RateLimitManager

if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    logger.setLevel(LOG_LEVEL)

    ch = logging.StreamHandler()
    ch.setLevel(LOG_LEVEL)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s.%(funcName)s.%(lineno)d  - [%(levelname)s]: %(message)s"
    )
    ch.setFormatter(formatter)

    logger.addHandler(ch)
else:
    logger = logging.getLogger(f"main.{__name__}")
    logger.setLevel(LOG_LEVEL)


class RateLimitManager:
    """Class to manage 429 and 418 errors."""

    def __init__(self):
        self._lock = Lock()
        self._retry_after = 0
        self._ban_until = 0

    def get_wait_time(self) -> float:
        current_time = time()
        if current_time < self._ban_until:
            return self._ban_until - current_time
        if current_time < self._retry_after:
            return self._retry_after - current_time
        return 0

    def set_rate_limit(self, retry_after):
        self._retry_after = time() + retry_after

    def set_ban(self, ban_duration):
        self._ban_until = time() + ban_duration


class CircuitBreaker:
    """Circuit Breaker class."""

    def __init__(self, max_failures=5, reset_time=300):
        self.failures = 0
        self.max_failures = max_failures
        self.reset_time = reset_time
        self.last_failure_time = None

    def record_failure(self) -> None:
        current_time = datetime.now()

        if self.last_failure_time:
            secs_since_reset = timedelta(seconds=self.reset_time)
            if (current_time - self.last_failure_time) > secs_since_reset:
                self.failures = 0

        self.failures += 1
        self.last_failure_time = current_time

    def is_open(self) -> bool:
        return self.failures >= self.max_failures

    def seconds_to_reset(self) -> float:
        if self.is_open():
            return (self.last_failure_time + self.reset_time) - time()
        else:
            return 0


@dataclass
class RateLimiter:
    """Rate limiter class"""

    weight_1m: int = 0
    weight_total: int = 0
    last_update: float = time()
    time_offset: float = 0.0
    throttle_counter: int = 0

    def update(self, weight_1m: int, weight_total: int):
        # reset at the start of each minute
        if self.seconds_to_next_full_minute() > 55:
            self.weight_1m = 0
            self.weight_total = 0

        # update values
        self.weight_1m = max(weight_1m, self.weight_1m)
        self.weight_total = max(weight_total, self.weight_total)
        self.last_update = time()

    def should_throttle(self) -> bool:
        return self.weight_1m > SOFT_LIMIT or self.weight_total > LONG_DELAY_THRESHHOLD

    def chill_out_bro(self) -> tuple[float, int]:
        # calculate the delay we need to prevent 'too many requests'
        seconds_left = self.seconds_to_next_full_minute() + 0.5

        # start throttling when the 1m weight goes above the soft limit
        base_delay = BASE_DELAY
        short_delay = 0.0

        if self.weight_1m >= SOFT_LIMIT:
            short_delay = base_delay + (
                seconds_left**2 / (RATE_LIMIT_BARRIER - (self.weight_1m - 1))
            )

        # wait for the next minute before proceeding when we approach
        # the API limit of 1200 used weight per one minute
        # but do not wait longer than 55 seconds (even though we try to sync to
        # to the server time, sometimes the delay was set to 59 or 60 seconds)
        if self.weight_1m >= HARD_LIMIT:
            short_delay = seconds_left if seconds_left < 55 else 5

        # handle the Raw Request Limit ... usually, this should never happen,
        # because if the above code works as intended then we will not reach
        # more than 6000 requests per 5 minutes
        add_delay = 0.0

        if self.weight_total > LONG_DELAY_THRESHHOLD:
            logger.info(f"increased total weight: {self.weight_total}")
            add_delay = (self.weight_total - LONG_DELAY_THRESHHOLD) / 50

        # add even more delay when we have used up more than 95%
        # of the weight per 5 minutes
        if self.weight_total > RAW_REQUEST_LIMIT * 0.95:
            logger.warning("approaching 5m limit ... increasing delay")
            base_delay = 10

        self.throttle_counter += 1  # just for logging purposes
        return short_delay + add_delay, self.throttle_counter

    def seconds_to_next_full_minute(self) -> float:
        adjusted_time = int(time() + self.time_offset)
        now = datetime.fromtimestamp(adjusted_time)
        return 60 - now.second


class BinanceClient:
    """Specialized Binance client for OHLCV data downloads."""

    def __init__(
        self,
        max_workers: int | None = None,
        max_retries: int | None = None,
        timeout: int = 10,
    ) -> None:
        self.max_workers = max_workers or MAX_WORKERS
        self.max_retries = max_retries or MAX_RETRIES
        self.timeout = aiohttp.ClientTimeout(total=timeout)

        self.rate_limiter = RateLimiter()
        self.circuit_breaker = CircuitBreaker()
        self.rate_limit_manager = RateLimitManager()
        self.semaphore = asyncio.Semaphore(self.max_workers)

    async def sync_server_time(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{BASE_URL}/api/v3/time") as response:
                server_time = (await response.json())["serverTime"] / 1000
                self.rate_limiter.time_offset = server_time - time()

    async def _get_chunk_periods(
        self, start: int, end: int, interval: str
    ) -> List[Tuple[int, int]]:
        chunk_size = 1000  # Maximum number of candles per request
        interval_ms = {
            "1m": 60000,
            "3m": 180000,
            "5m": 300000,
            "15m": 900000,
            "30m": 1800000,
            "1h": 3600000,
            "2h": 7200000,
            "4h": 14400000,
            "6h": 21600000,
            "8h": 28800000,
            "12h": 43200000,
            "1d": 86400000,
            "3d": 259200000,
            "1w": 604800000,
            "1M": 2592000000,
        }

        step = chunk_size * interval_ms[interval]
        chunks = []
        chunk_start = start

        while chunk_start < end:
            chunk_end = min(chunk_start + step, end)
            chunks.append((chunk_start, chunk_end))
            chunk_start = chunk_end + 1

        return chunks

    async def _fetch_ohlcv_chunk(
        self,
        session: aiohttp.ClientSession,
        worker_id: str,
        symbol: str,
        interval: str,
        start: int,
        end: int,
    ) -> List[List[Any]]:

        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": start,
            "endTime": end,
            "limit": KLINES_LIMIT,
        }

        for attempt in range(self.max_retries):
            try:
                async with self.semaphore:
                    # check with the Rate Limit Manager if we need to wait
                    # because we exceeded the API rate limit or got banned
                    if wait_time := self.rate_limit_manager.get_wait_time() > 0:
                        logger.warning(
                            "[%s] Rate limit in effect. Waiting for %s seconds.",
                            worker_id,
                            round(wait_time),
                        )
                        await asyncio.sleep(wait_time)

                    # check with the CircuitBreaker if we need to wait because
                    # too many erros occured
                    if self.circuit_breaker.is_open():
                        logger.warning(
                            "Circuit breaker is open. Waiting before retrying..."
                        )
                        await asyncio.sleep(self.circuit_breaker.seconds_to_reset())

                    # check with the RateLimiter if we need to throttle
                    # our rate of requests and wait to avoid hitting the
                    # API rate limit(s)
                    delay, count = 0.0, 0
                    if self.rate_limiter.should_throttle():
                        delay, count = self.rate_limiter.chill_out_bro()
                        logger.info(
                            "[%s] throttling %s: %s",
                            worker_id,
                            count,
                            round(delay, 2),
                        )
                        await asyncio.sleep(delay)

                    # send the downlaod request to the API
                    async with session.get(
                        f"{BASE_URL}{API_ENDPOINT}",
                        params=params,
                    ) as response:
                        self.rate_limiter.update(
                            int(response.headers.get("x-mbx-used-weight-1m", 0)),
                            int(response.headers.get("x-mbx-used-weight", 0)),
                        )

                        # simulates 429 errors (too many requests) if the
                        # SIMULATE_429_ERROS switch has been set (at top of file)
                        if SIMULATE_429_ERRORS and random.random() > 0.99:
                            response.status = 429

                        # handle a 429 error (too many requests)
                        if response.status == 429:
                            logger.warning("[%s] Hit a 429 error!" % worker_id)

                            # get the seconds to wait for a retry from
                            # the headers of the response, or set it to
                            # 10 if we are only simulating a 429
                            if SIMULATE_429_ERRORS:
                                retry_after = 10
                            else:
                                retry_after = int(
                                    response.headers.get("Retry-After", 60)
                                )

                            self.rate_limit_manager.set_rate_limit(retry_after)
                            return await self._fetch_ohlcv_chunk(
                                session,
                                worker_id,
                                symbol,
                                interval,
                                start,
                                end,
                            )

                        # handle a 418 error (= banned)
                        if response.status == 418:
                            retry_after = int(response.headers.get("Retry-After", 120))
                            logger.error(
                                "[%s] We are BANNED for %s seconds"
                                % (worker_id, retry_after)
                            )
                            self.rate_limit_manager.set_ban(retry_after)
                            return await self._fetch_ohlcv_chunk(
                                session,
                                worker_id,
                                symbol,
                                interval,
                                start,
                                end,
                            )

                        response.raise_for_status()

                        logger.info(
                            "[%s] Request successful. Weight: %s-%s",
                            worker_id,
                            self.rate_limiter.weight_1m,
                            self.rate_limiter.weight_total,
                        )

                        return await response.json()

            except asyncio.TimeoutError:
                logger.warning("[%s] Request timed out. Retrying..." % worker_id)
                self.circuit_breaker.record_failure()
                if attempt == self.max_retries - 1:
                    raise

            except (aiohttp.ClientResponseError, Exception) as e:
                logger.error(
                    "[%s] Error: %s for %s. Retrying in %s seconds...",
                    worker_id,
                    e,
                    symbol,
                    f"{wait_time:.2f}",
                )
                self.circuit_breaker.record_failure()
                if attempt == self.max_retries - 1:
                    raise
                wait_time = (2**attempt) + random.uniform(0, 1)
                await asyncio.sleep(wait_time)

        logger.warning(
            "no result for %s after retrying %s times" % (symbol, self.max_retries)
        )
        return []

    async def get_ohlcv(
        self, symbol: str, interval: str, start: int, end: int
    ) -> List[List[Any]]:
        """
        Asynchronously fetches OHLCV (Open, High, Low, Close, Volume)
        data for a given symbol and time range.

        This method breaks down the requested time range into chunks,
        fetches data for each chunk in parallel,filters out any errors,
        and returns the combined and sorted results.

        Arguments:
        ----------
        symbol: str
            The trading pair symbol (e.g., 'BTCUSDT').
        interval: str
            The candlestick interval (e.g., '1m', '1h', '1d').
        start:int
            The start time in milliseconds since the Unix epoch.
        end: int
            The end time in milliseconds since the Unix epoch.

        Returns:
        --------
        List: [List[Any]]
            A list of OHLCV data points, where each data point is a list
            containing [timestamp, open, high, low, close, volume, ...]
            in that order. The list is sorted by timestamp in ascending
            order.
        """
        chunks = await self._get_chunk_periods(start, end, interval)

        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            tasks = [
                self._fetch_ohlcv_chunk(
                    session,
                    f"{symbol}_{i}",
                    symbol,
                    interval,
                    chunk_start,
                    chunk_end,
                )
                for i, (chunk_start, chunk_end) in enumerate(chunks)
            ]
            results = await asyncio.gather(*tasks)

        # Filter out any exceptions and log them
        filtered_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Error in chunk {i} for {symbol}: {result}")
            else:
                filtered_results.extend(result)

        # Sort the results by open time (first element in each candle)
        sorted_results = sorted(filtered_results, key=lambda x: x[0])

        return sorted_results

    async def download_ohlcv_data(
        self, symbols: List[str], interval: str, start: int, end: int
    ) -> Dict[str, List[List[Any]]]:
        """
        Asynchronously download OHLCV (Open, High, Low, Close, Volume)
        data for multiple symbols.

        This function creates concurrent tasks to fetch OHLCV data for
        each symbol in the provided list, over the specified time range
        and interval.

        Parameters:
        -----------
        symbols : List[str]
            A list of trading pair symbols (e.g., ['BTCUSDT', 'ETHUSDT']).
        interval : str
            The candlestick interval (e.g., '1m', '1h', '1d').
        start : int
            The start time in milliseconds since the Unix epoch.
        end : int
            The end time in milliseconds since the Unix epoch.

        Returns:
        --------
        Dict[str, List[List[Any]]]
            A dictionary where each key is a symbol and the corresponding
            value is a list of OHLCV data points. Each data point is a
            list containing [timestamp, open, high, low, close, volume, ...]
            in that order.
        """
        tasks = [self.get_ohlcv(symbol, interval, start, end) for symbol in symbols]
        results = await asyncio.gather(*tasks)
        return dict(zip(symbols, results))


# Usage
async def main():
    client = BinanceClient()
    symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT", "XLMUSDT"]
    interval = "15m"
    start = int(time() * 1000) - 5 * 365 * 24 * 60 * 60 * 1000  # 5 years ago
    end = int(time() * 1000)

    st = time()

    data = await client.download_ohlcv_data(symbols, interval, start, end)

    et = int((time() - st) * 1000)
    no_of_klines = sum(len(v) for v in data.values())
    print(f"got {no_of_klines} klines in {et}ms")

    for symbol, ohlcv in data.items():
        print(f"{symbol}: {len(ohlcv)} candles downloaded")


if __name__ == "__main__":
    asyncio.run(main())
