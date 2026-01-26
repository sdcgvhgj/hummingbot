import os
import asyncio
from decimal import Decimal
from typing import Dict, List, Set
from datetime import datetime, timedelta
from collections import deque
import time
import json

import pandas as pd
from pydantic import Field, field_validator

from hummingbot.client.ui.interface_utils import format_df_for_printout
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.clock import Clock
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PriceType, TradeType
from hummingbot.core.event.events import FundingPaymentCompletedEvent
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig, TripleBarrierConfig
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction
from hummingbot.strategy_v2.models.executors_info import ExecutorInfo
from hummingbot.core.trading_core import TradingCore

TOKEN_FAILURE_COOL_DOWN_COUNT = 60 * 60 * 24
CREATE_ACTION_COOL_DOWN_COUNT = 60 * 5 # 5 minutes

# ------------------------
# Lightweight process-wide memory snapshot monitor
# - Uses stdlib tracemalloc for allocation snapshots (low overhead)
# - Optionally uses psutil/Pympler if available (best-effort)
# - Supports periodic snapshots, RSS growth threshold triggers, and SIGUSR1 manual dump
# Dumps go to logs/mem/*.log
# ------------------------
try:
    import tracemalloc  # stdlib
    _HAS_TRACEMALLOC = True
except Exception:
    _HAS_TRACEMALLOC = False

import threading
import gc
import sys
import re
from pathlib import Path

try:
    import signal  # not always available on Windows, fine on Linux/WSL2
    _HAS_SIGNAL = True
except Exception:
    _HAS_SIGNAL = False

try:
    import psutil  # optional
    _HAS_PSUTIL = True
except Exception:
    _HAS_PSUTIL = False

try:
    # Optional deep object summary
    from pympler import muppy, summary, asizeof  # type: ignore
    _HAS_PYMPLER = True
except Exception:
    _HAS_PYMPLER = False


class _MemoryMonitor:
    def __init__(self, logger, dump_dir: str = None, interval_sec: int = 60, topn: int = 30,
                 rss_threshold_mb: int = 0):
        self._logger = logger
        self._interval_sec = max(1, int(interval_sec))
        self._topn = max(5, int(topn))
        self._rss_threshold_bytes = int(rss_threshold_mb) * 1024 * 1024 if rss_threshold_mb else 0
        self._dump_dir = Path(dump_dir or (Path.cwd() / "logs" / "mem"))
        self._dump_dir.mkdir(parents=True, exist_ok=True)
        self._thread = None
        self._stop = threading.Event()
        self._last_rss = 0
        self._last_snapshot = None
        self._tracemalloc_started = False
        # On-demand tracemalloc session control (SIGUSR1 triggers a 5min window)
        self._tm_session_deadline = 0.0  # monotonic deadline when to stop
        self._tm_pending_begin = False   # set by signal handler; consumed in loop

    def start(self):
        # Install a SIGUSR1 handler to start a 5-minute tracemalloc session on demand
        if _HAS_SIGNAL:
            try:
                signal.signal(signal.SIGUSR1, self._handle_sigusr1)
            except Exception:
                # Not in main thread or not supported
                pass

        self._thread = threading.Thread(target=self._run_loop, name="MemoryMonitor", daemon=True)
        self._thread.start()
        mode = "on-demand" if _HAS_TRACEMALLOC else "no-tracemalloc"
        self._logger().info(f"[mem] Memory monitor started ({mode}); send SIGUSR1 to trace for 5min")

    def stop(self):
        try:
            self._stop.set()
        except Exception:
            pass

    def _handle_sigusr1(self, signum, frame):
        # Defer heavy work to background thread
        try:
            self._tm_pending_begin = True
        except Exception as e:
            self._logger().warning(f"[mem] SIGUSR1 handling failed: {e}")

    def _begin_tracemalloc_session(self, duration_sec: int = 300):
        if not _HAS_TRACEMALLOC:
            self._logger().warning("[mem] tracemalloc unavailable; cannot start session")
            return
        try:
            import time as _time
            now = _time.monotonic()
            if not self._tracemalloc_started:
                # Keep small frame depth to reduce overhead
                tracemalloc.start(5)
                self._tracemalloc_started = True
                self._last_snapshot = None  # reset diff base for this session
                self._logger().info("[mem] tracemalloc session started (5min)")
            else:
                self._logger().info("[mem] tracemalloc session extended")
            self._tm_session_deadline = now + max(1, int(duration_sec))
        except Exception as e:
            self._logger().warning(f"[mem] Failed to start tracemalloc session: {e}")

    def _current_rss(self) -> int:
        if _HAS_PSUTIL:
            try:
                return psutil.Process().memory_info().rss
            except Exception:
                pass
        # Fallback: Linux /proc/self/status VmRSS
        try:
            with open("/proc/self/status", "r") as f:
                text = f.read()
            m = re.search(r"VmRSS:\\s+(\\d+)\\s+kB", text)
            if m:
                return int(m.group(1)) * 1024
        except Exception:
            pass
        # Last resort: return 0 if unknown
        return 0

    def _run_loop(self):
        # Initial baseline
        self._last_rss = self._current_rss()
        while not self._stop.is_set():
            try:
                # Handle on-demand session begin triggered by SIGUSR1
                if self._tm_pending_begin:
                    self._tm_pending_begin = False
                    self._begin_tracemalloc_session(300)
                    try:
                        self._dump_snapshot(reason="tm_begin")
                    except Exception as e:
                        self._logger().warning(f"[mem] tm_begin snapshot failed: {e}")

                # Periodic dump
                self._dump_snapshot(reason="interval")

                # Threshold check
                if self._rss_threshold_bytes:
                    cur_rss = self._current_rss()
                    if self._last_rss and cur_rss - self._last_rss >= self._rss_threshold_bytes:
                        self._dump_snapshot(reason=f"rss+{(cur_rss - self._last_rss) / (1024*1024):.1f}MB")
                        self._last_rss = cur_rss

                # Stop tracemalloc session if deadline reached
                if self._tracemalloc_started:
                    import time as _time
                    if self._tm_session_deadline and _time.monotonic() >= self._tm_session_deadline:
                        try:
                            self._dump_snapshot(reason="tm_end")
                        except Exception as e:
                            self._logger().warning(f"[mem] tm_end snapshot failed: {e}")
                        try:
                            tracemalloc.stop()
                            self._logger().info("[mem] tracemalloc session stopped")
                        except Exception as e:
                            self._logger().warning(f"[mem] Failed to stop tracemalloc: {e}")
                        finally:
                            self._tracemalloc_started = False
                            self._last_snapshot = None
                            self._tm_session_deadline = 0.0
            except Exception as e:
                self._logger().warning(f"[mem] monitor loop error: {e}")
            finally:
                self._stop.wait(self._interval_sec)

    def _dump_snapshot(self, reason: str):
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        path = self._dump_dir / f"memsnap_{ts}_{reason}.log"
        try:
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(f"# Memory snapshot @ {ts} UTC (reason={reason})\n")

                # RSS
                rss = self._current_rss()
                if rss:
                    fh.write(f"RSS: {rss/ (1024*1024):.2f} MB\n")

                # tracemalloc stats (only if session active)
                if _HAS_TRACEMALLOC and self._tracemalloc_started:
                    current, peak = tracemalloc.get_traced_memory()
                    fh.write(f"tracemalloc current: {current/(1024*1024):.2f} MB, peak: {peak/(1024*1024):.2f} MB\n")
                    snap = tracemalloc.take_snapshot()
                    top_lines = snap.statistics('traceback')[: self._topn]
                    fh.write("\nTop allocations by line (tracemalloc):\n")
                    for i, stat in enumerate(top_lines, 1):
                        # fh.write(f"{i:2d}. {stat.traceback.format()[-1].strip()} | size={stat.size/1024:.1f} KiB | count={stat.count}\n")
                        fh.write(f"{i:2d}. {stat}\n")
                        for j, frame in enumerate(stat.traceback):
                            fh.write(f"    [{j:2d}] {frame.filename}:{frame.lineno}\n")

                    # Diff with previous snapshot (where growing?)
                    if self._last_snapshot is not None:
                        fh.write("\nDiff since last snapshot (by line):\n")
                        for i, stat in enumerate(snap.compare_to(self._last_snapshot, 'traceback')[: self._topn], 1):
                            # sign = "+" if stat.size_diff >= 0 else "-"
                            # tb = stat.traceback.format()[-1].strip() if stat.traceback else "<unknown>"
                            # fh.write(f"{i:2d}. {tb} | d_size={sign}{abs(stat.size_diff)/1024:.1f} KiB | d_count={stat.count_diff}\n")
                            fh.write(f"{i:2d}. {stat}\n")
                            for j, frame in enumerate(stat.traceback):
                                fh.write(f"    [{j:2d}] {frame.filename}:{frame.lineno}\n")
                    self._last_snapshot = snap

                # Object type summary (Pympler if available)
                fh.write("\nObject summary by type:\n")
                if _HAS_PYMPLER:
                    try:
                        all_objs = muppy.get_objects()
                        sum1 = summary.summarize(all_objs)
                        formatted_lines = summary.format_(sum1)
                        fh.write("\n".join(formatted_lines) + "\n")
                    except Exception as e:
                        fh.write(f"<pympler failed: {e}>\n")
                else:
                    # Fallback: approximate counts from gc
                    try:
                        counts = {}
                        for o in gc.get_objects():
                            t = type(o).__name__
                            counts[t] = counts.get(t, 0) + 1
                        top_types = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)[: self._topn]
                        for t, c in top_types:
                            fh.write(f"{t}: {c}\n")
                    except Exception as e:
                        fh.write(f"<gc summary failed: {e}>\n")

                # Heaviest containers (shallow) to guess "which variables"
                fh.write("\nLargest containers (shallow, best-effort):\n")
                try:
                    import builtins
                    sized = []
                    # if self._tracemalloc_started:
                    #     for o in gc.get_objects():
                    #         try:
                    #             t = type(o)
                    #             if t in (list, dict, set, tuple):
                    #                 size = sys.getsizeof(o)
                    #                 ln = len(o) if hasattr(o, '__len__') else 0
                    #                 mod = getattr(t, '__module__', '')
                    #                 # Attempt to retrieve o's variable name from globals or locals
                    #                 var_name = None
                    #                 try:
                    #                     for scope in (globals(), locals()):
                    #                         for k, v in scope.items():
                    #                             if v is o and not k == 'o':
                    #                                 var_name = k
                    #                                 break
                    #                         if var_name:
                    #                             break
                    #                 except Exception:
                    #                     var_name = None
                    #                 sized.append((size, ln, t.__name__, mod, o, var_name))
                    #         except Exception:
                    #             continue
                    sized.sort(key=lambda x: x[0], reverse=True)
                    for i, (size, ln, tname, mod, o, var_name) in enumerate(sized[: self._topn], 1):
                        preview = None
                        try:
                            r = repr(list(o)[:3]) if isinstance(o, (list, tuple, set)) else repr(list(o.items())[:3]) if isinstance(o, dict) else repr(o)
                            preview = (r[:120] + '...') if len(r) > 120 else r
                        except Exception:
                            preview = '<unrepr>'
                        fh.write(f"{i:2d}. {tname} len={ln} shallow={size/1024:.1f} KiB mod={mod} sample={preview} var_name={var_name}\n")
                except Exception as e:
                    fh.write(f"<largest containers failed: {e}>\n")

            self._logger().info(f"[mem] snapshot written: {path}")
        except Exception as e:
            self._logger().warning(f"[mem] failed to write snapshot: {e}")

class FundingRateArbitrageConfig(StrategyV2ConfigBase):
    script_file_name: str = os.path.basename(__file__)
    candles_config: List[CandlesConfig] = []
    controllers_config: List[str] = []
    markets: Dict[str, Set[str]] = {}
    leverage: int = Field(
        default=20, gt=0,
        json_schema_extra={"prompt": lambda mi: "Enter the leverage (e.g. 20): ", "prompt_on_new": True},
    )
    liquidation_buffer_pct: Decimal = Field(
        default=0.05,
        json_schema_extra={
            "prompt": lambda mi: "Stop if price is within X pct of estimated liquidation (e.g. 0.05): ",
            "prompt_on_new": True}
    )
    min_trade_profitability: Decimal = Field(
        default=0.001,
        json_schema_extra={
            "prompt": lambda mi: "Enter the min trade profitability to enter in a position (e.g. 0.001): ",
            "prompt_on_new": True}
    )
    min_funding_profitability: Decimal = Field(
        default=0.001,
        json_schema_extra={
            "prompt": lambda mi: "Enter the min funding rate profitability to enter in a position (e.g. 0.001): ",
            "prompt_on_new": True}
    )
    min_price_diff: Decimal = Field(
        default=0.001,
        json_schema_extra={
            "prompt": lambda mi: "Enter the min price diff to enter in a position (e.g. 0.001): ",
            "prompt_on_new": True}
    )
    min_take_profit: Decimal = Field(
        default=0.001,
        json_schema_extra={
            "prompt": lambda mi: "Enter the min take profit threshold to close positions (e.g. 0.001): ",
            "prompt_on_new": True}
    )
    connectors: Set[str] = Field(
        default="hyperliquid_perpetual,binance_perpetual",
        json_schema_extra={
            "prompt": lambda mi: "Enter the connectors separated by commas (e.g. hyperliquid_perpetual,binance_perpetual): ",
            "prompt_on_new": True}
    )
    tokens: Set[str] = Field(
        default="WIF,FET",
        json_schema_extra={"prompt": lambda mi: "Enter the tokens separated by commas (e.g. WIF,FET): ", "prompt_on_new": True},
    )
    position_size_quote: Decimal = Field(
        default=100,
        json_schema_extra={
            "prompt": lambda mi: "Enter the position size in quote asset (e.g. order amount 100 will open 100 long on hyperliquid and 100 short on binance): ",
            "prompt_on_new": True
        }
    )
    max_time_to_next_funding: Decimal = Field(
        default=240,
        json_schema_extra={
            "prompt": lambda mi: "Enter x such that only open when next funding is within x minutes (e.g. 240): ",
            "prompt_on_new": True}
    )
    min_time_to_next_funding: Decimal = Field(
        default=10,
        json_schema_extra={
            "prompt": lambda mi: "Enter x such that only open when next funding is at least x minutes (e.g. 10): ",
            "prompt_on_new": True}
    )
    max_hold_time_minutes: int = Field(
        default=120,
        json_schema_extra={
            "prompt": lambda mi: "Force-close positions after holding this many minutes (e.g. 120): ",
            "prompt_on_new": True}
    )
    # Dynamic Top-K scanning controls
    dynamic_topk_enabled: bool = Field(
        default=False,
        json_schema_extra={
            "prompt": lambda mi: "Enable dynamic Top-K token scanning via REST? (true/false): ",
            "prompt_on_new": True}
    )
    topk: int = Field(
        default=10,
        json_schema_extra={
            "prompt": lambda mi: "How many tokens to keep subscribed via WS (K): ",
            "prompt_on_new": True}
    )
    scan_interval_hours: int = Field(
        default=12,
        json_schema_extra={
            "prompt": lambda mi: "Scan interval in hours (e.g. 12): ",
            "prompt_on_new": True}
    )
    # Memory monitor controls
    memory_monitor_enabled: bool = Field(
        default=True,
        json_schema_extra={
            "prompt": lambda mi: "Enable memory monitor (true/false): ",
            "prompt_on_new": True}
    )
    memory_snapshot_interval_sec: int = Field(
        default=300,
        json_schema_extra={
            "prompt": lambda mi: "Memory snapshot interval seconds (e.g. 300): ",
            "prompt_on_new": True}
    )
    memory_snapshot_topn: int = Field(
        default=30,
        json_schema_extra={
            "prompt": lambda mi: "Memory snapshot top-N entries (e.g. 30): ",
            "prompt_on_new": True}
    )
    memory_rss_threshold_mb: int = Field(
        default=256,
        json_schema_extra={
            "prompt": lambda mi: "Trigger snapshot when RSS grows by MB (0=disable, e.g. 256): ",
            "prompt_on_new": True}
    )
    # Hard guard to throttle trading and force a snapshot when RSS is too high
    memory_guard_limit_mb: int = Field(
        default=0,
        json_schema_extra={
            "prompt": lambda mi: "Hard RSS guard in MB to pause/limit trading (0=disable, e.g. 2300): ",
            "prompt_on_new": True}
    )
    memory_guard_check_interval_sec: int = Field(
        default=30,
        json_schema_extra={
            "prompt": lambda mi: "RSS guard check interval seconds (e.g. 30): ",
            "prompt_on_new": True}
    )
    # Telegram 通知（可选）
    telegram_bot_token: str = Field(
        default="",
        json_schema_extra={
            "prompt": lambda mi: "Telegram bot token (optional, leave empty to disable): ",
            "prompt_on_new": True}
    )
    telegram_chat_id: str = Field(
        default="",
        json_schema_extra={
            "prompt": lambda mi: "Telegram chat id (optional, leave empty to disable): ",
            "prompt_on_new": True}
    )

    # Price smoothing controls
    ema_ticks: int = Field(
        default=5,
        json_schema_extra={
            "prompt": lambda mi: "EMA window in ticks for price smoothing (e.g. 5): ",
            "prompt_on_new": True}
    )

    # Consecutive confirmation controls
    condition_consecutive_required: int = Field(
        default=3,
        json_schema_extra={
            "prompt": lambda mi: "Open only after N consecutive seconds meeting condition (e.g. 3): ",
            "prompt_on_new": True}
    )

    @field_validator("connectors", "tokens", mode="before")
    @classmethod
    def validate_sets(cls, v):
        if isinstance(v, str):
            return set(v.split(","))
        return v


class FundingRateArbitrage(StrategyV2Base):
    quote_markets_map = {
        "hyperliquid_perpetual": "USD",
        "binance_perpetual": "USDT"
    }
    funding_payment_interval_map = {
        "binance_perpetual": 60 * 60 * 8,
        "hyperliquid_perpetual": 60 * 60 * 1,
        "okx_perpetual": 60 * 60 * 8,
        "bybit_perpetual": 60 * 60 * 8,
    }
    position_mode_map = {
        "hyperliquid_perpetual": PositionMode.ONEWAY,
        "okx_perpetual" : PositionMode.HEDGE,
        "gate_io_perpetual": PositionMode.ONEWAY,
    }

    @classmethod
    def get_trading_pair_for_connector(cls, token, connector):
        return f"{token}-{cls.quote_markets_map.get(connector, 'USDT')}"

    @classmethod
    def init_markets(cls, config: FundingRateArbitrageConfig):
        markets = {}
        for connector in config.connectors:
            trading_pairs = {cls.get_trading_pair_for_connector(token, connector) for token in config.tokens}
            markets[connector] = trading_pairs
        cls.markets = markets

    def __init__(self, connectors: Dict[str, ConnectorBase], config: FundingRateArbitrageConfig):
        super().__init__(connectors, config)
        self.config = config
        self.active_funding_arbitrages = {}
        self.stopped_funding_arbitrages = {token: [] for token in self.config.tokens}
        self.tokens_supported_exchange_map = dict()
        self.token_failure_cool_down = {token: 0 for token in self.config.tokens}
        self.create_action_cool_down = 0
        self._dynamic_scan_task = None
        self._dynamic_topk_tokens = set(self.config.tokens)
        self._latest_topk_debug = []
        self.is_stopping_creating_actions = False
        self._mem_monitor = None
        self._status_dump_thread = None
        self._status_dump_stop = threading.Event()
        self._last_mem_guard_check = 0.0
        self._mem_guard_triggered = False
        self._last_insufficient_balance_alert_ts = 0.0
        self._last_open_position_alert_ts = 0.0
        self._last_daily_reminder_ts = 0.0
        self._last_daily_total_balance = None
        self._last_daily_stopped_counts = {token: 0 for token in self.config.tokens}

        # EMA price smoothing state
        try:
            ticks = int(getattr(self.config, "ema_ticks", 5))
        except Exception:
            ticks = 5
        self._ema_prices = {}
        self._ema_alpha = (Decimal(2) / Decimal(ticks + 1)) if ticks and ticks > 1 else Decimal(1)

        # Per-token condition hit queues for consecutive confirmation
        self._cond_hits_map = {}
        self._stop_cond_hits_map = {}

        self.status_active_arbitrage_info = None
        self.status_stopped_arbitrage_info = None
        self._fee_cache = {}

    def start(self, clock: Clock, timestamp: float) -> None:
        """
        Start the strategy.
        :param clock: Clock to use.
        :param timestamp: Current time.
        """
        self._last_timestamp = timestamp
        self.apply_initial_setting()
        # Start memory monitor (process-wide)
        try:
            if getattr(self.config, "memory_monitor_enabled", False):
                self._mem_monitor = _MemoryMonitor(
                    logger=self.logger,
                    interval_sec=int(getattr(self.config, "memory_snapshot_interval_sec", 600)),
                    topn=int(getattr(self.config, "memory_snapshot_topn", 10)),
                    rss_threshold_mb=int(getattr(self.config, "memory_rss_threshold_mb", 256)),
                )
                self._mem_monitor.start()
        except Exception as e:
            self.logger().warning(f"[mem] Failed to start memory monitor: {e}")
        # Kick off dynamic scanner if enabled
        if getattr(self.config, "dynamic_topk_enabled", False):
            try:
                if self._dynamic_scan_task is None or self._dynamic_scan_task.done():
                    self.logger().info("[dynamic-topk] Starting background scanner loop...")
                    self._dynamic_scan_task = asyncio.create_task(self._dynamic_scan_loop())
            except Exception as e:
                self.logger().error(f"[dynamic-topk] Failed to start scanner: {e}")
        # Start status dump thread (write format_status to disk every 60s)
        try:
            if self._status_dump_thread is None or self._status_dump_thread.done():
                self.logger().info("[status-dump] Status dump thread started (interval=60s)")
                self._status_dump_thread = asyncio.create_task(self._status_dump_loop())
        except Exception as e:
            self.logger().warning(f"[status-dump] Failed to start status dump thread: {e}")

    def token_supported_exchange(self, token: str):
        if token in self.tokens_supported_exchange_map:
            return self.tokens_supported_exchange_map[token]
        supported_connectors = []
        for connector_name, connector in self.connectors.items():
            trading_pair = self.get_trading_pair_for_connector(token, connector_name)
            if trading_pair in connector.trading_pairs:
                supported_connectors.append(connector_name)
        if len(supported_connectors) == 0:
            self.logger().warning(f"Token {token} is supported by NO exchanges")
        else:
            self.logger().info(f"Token {token} is supported by {','.join(supported_connectors)}")
        self.tokens_supported_exchange_map[token] = supported_connectors
        return supported_connectors


    def apply_initial_setting(self):
        for connector_name, connector in self.connectors.items():
            if self.is_perpetual(connector_name):
                connector.set_position_mode(self.position_mode_map.get(connector_name, PositionMode.HEDGE))
                for trading_pair in self.market_data_provider.get_trading_pairs(connector_name):
                    connector.set_leverage(trading_pair, self.config.leverage)

    def tokens_for_trading(self):
        return self._dynamic_topk_tokens if getattr(self.config, "dynamic_topk_enabled", False) else self.config.tokens

    def get_funding_info_by_token(self, token):
        """
        This method provides the funding rates across all the connectors
        """
        funding_rates = {}
        for connector_name in self.token_supported_exchange(token):
            connector = self.connectors[connector_name]
            trading_pair = self.get_trading_pair_for_connector(token, connector_name)
            try:
                funding_rates[connector_name] = connector.get_funding_info(trading_pair)
            except Exception as e:
                # Keep running even if one connector fails to respond
                self.logger().warning(f"Failed to get funding info for {trading_pair} on {connector_name}: {e}")
        return funding_rates

    def get_price_and_fee_with_cache(self, prices_and_fees_cache: Dict, connector_name, token: str, side: TradeType):
        if connector_name in prices_and_fees_cache:
            return prices_and_fees_cache[connector_name]

        trading_pair = self.get_trading_pair_for_connector(token, connector_name)
        raw_price = Decimal(self.market_data_provider.get_price_for_quote_volume(
            connector_name=connector_name,
            trading_pair=trading_pair,
            quote_volume=self.config.position_size_quote,
            is_buy=side == TradeType.BUY,
        ).result_price)
        price = self._ema_update_and_get(connector_name, trading_pair, raw_price)

        if connector_name not in self._fee_cache:
            self._fee_cache[connector_name] = self.connectors[connector_name].get_fee(
                base_currency=trading_pair.split("-")[0],
                quote_currency=trading_pair.split("-")[1],
                order_type=OrderType.MARKET,
                order_side=TradeType.BUY,
                amount=self.config.position_size_quote / price,
                price=price,
                is_maker=False,
                position_action=PositionAction.OPEN
            ).percent

        fee = self._fee_cache[connector_name]
        prices_and_fees_cache[connector_name] = (price, fee)
        return (price, fee)

    def get_most_trade_profitable_combination(self, prices_and_fees_cache: Dict, funding_info_report: Dict, token: str,
                                                funding_time_check: bool = True):
        # Remove connectors that are far away from funding time
        valid_connectors = []
        for connector in funding_info_report:
            time_to_funding = funding_info_report[connector].next_funding_utc_timestamp - self.current_timestamp
            if time_to_funding / 60 < self.config.max_time_to_next_funding and time_to_funding / 60 > self.config.min_time_to_next_funding:
                valid_connectors.append(connector)

        if not funding_time_check:
            valid_connectors = list(funding_info_report.keys())

        # TODO: computation delay mesure

        # Find best combination
        best_combination = None
        highest_profitability = Decimal(-100)
        for connector_1 in valid_connectors:
            for connector_2 in valid_connectors:
                if connector_1 != connector_2:
                    time_to_funding_1 = funding_info_report[connector_1].next_funding_utc_timestamp - self.current_timestamp
                    time_to_funding_2 = funding_info_report[connector_2].next_funding_utc_timestamp - self.current_timestamp
                    if funding_time_check and abs(time_to_funding_1 - time_to_funding_2) > 60:
                        continue
                    if not self._funding_intervals_match(funding_info_report, connector_1, connector_2):
                        continue
                    price_1, fee_1 = self.get_price_and_fee_with_cache(prices_and_fees_cache, connector_1, token, TradeType.BUY)
                    price_2, fee_2 = self.get_price_and_fee_with_cache(prices_and_fees_cache, connector_2, token, TradeType.SELL)
                    rate_1 = funding_info_report[connector_1].rate
                    rate_2 = funding_info_report[connector_2].rate
                    # p2 = 1.1 p1
                    # buy 10u / p1 amount at price p1, costs 10u
                    # sell 10u / p1 amount at price p2, returns 10u / p1 * p2 = 1.1 * 10u = 11u
                    # p2' = p1
                    # sell 10u / p1 amount at price p1, returns 10u
                    # buy 10u / p1 amount at price p2', costs 10u / p1 * p2' = 10u
                    # pnl_percent = p2 / p1 - 1 = (p2 - p1) / p1
                    i_price_1 = funding_info_report[connector_1].index_price
                    i_price_2 = funding_info_report[connector_2].index_price
                    i_price_diff = max(0, i_price_2 - i_price_1)
                    price_profit = (price_2 - price_1 - i_price_diff) / price_1
                    funding_rate_profit = rate_2 - rate_1
                    trade_profit = price_profit + funding_rate_profit - fee_1 * 2 - fee_2 * 2
                    if float(trade_profit) > float(highest_profitability):
                        trade_side = TradeType.BUY
                        highest_profitability = trade_profit
                        best_combination = (connector_1, connector_2, trade_side, trade_profit, \
                                            rate_1, rate_2, price_1, price_2, fee_1, fee_2, i_price_diff)
        return best_combination

    def _ema_key(self, connector_name: str, trading_pair: str) -> str:
        return f"{connector_name}|{trading_pair}"

    def _ema_update_and_get(self, connector_name: str, trading_pair: str, new_price: Decimal) -> Decimal:
        if self._ema_alpha == Decimal(1):
            return new_price
        key = self._ema_key(connector_name, trading_pair)
        old = self._ema_prices.get(key)
        if old is None:
            ema = new_price
        else:
            ema = self._ema_alpha * new_price + (Decimal(1) - self._ema_alpha) * old
        self._ema_prices[key] = ema
        return ema

    def get_best_combination_by_heuristic(self, prices_and_fees_cache: Dict, funding_info_report: Dict, token: str,
                                            funding_time_check: bool = True):
        valid_connectors = []
        for connector in funding_info_report:
            time_to_funding = funding_info_report[connector].next_funding_utc_timestamp - self.current_timestamp
            if time_to_funding / 60 < self.config.max_time_to_next_funding and time_to_funding / 60 > self.config.min_time_to_next_funding:
                valid_connectors.append(connector)

        if not funding_time_check:
            valid_connectors = list(funding_info_report.keys())

        best_score = None
        best = None
        for connector_1 in valid_connectors:
            for connector_2 in valid_connectors:
                if connector_1 == connector_2:
                    continue
                t1 = funding_info_report[connector_1].next_funding_utc_timestamp - self.current_timestamp
                t2 = funding_info_report[connector_2].next_funding_utc_timestamp - self.current_timestamp
                if funding_time_check and abs(t1 - t2) > 60:
                    continue
                if not self._funding_intervals_match(funding_info_report, connector_1, connector_2):
                    continue
                price_1, fee_1 = self.get_price_and_fee_with_cache(prices_and_fees_cache, connector_1, token, TradeType.BUY)
                price_2, fee_2 = self.get_price_and_fee_with_cache(prices_and_fees_cache, connector_2, token, TradeType.SELL)
                rate_1 = funding_info_report[connector_1].rate
                rate_2 = funding_info_report[connector_2].rate

                i_price_1 = funding_info_report[connector_1].index_price
                i_price_2 = funding_info_report[connector_2].index_price
                i_price_diff = max(0, i_price_2 - i_price_1)

                # 启发式打分：单位时间的预期收益
                time_to_funding = max(Decimal(60), Decimal(max(t1, t2)))  # 至少按60秒防止分母过小
                score = self.heuristic_profitability_evaluation(price_1, price_2, fee_1, fee_2, rate_1, rate_2, time_to_funding, i_price_diff)

                # 交易期望收益（用于后续阈值判断）
                price_profit = (price_2 - price_1 - i_price_diff) / price_1
                funding_rate_profit = rate_2 - rate_1
                trade_profit = price_profit + funding_rate_profit - fee_1 * 2 - fee_2 * 2

                if best_score is None or float(score) > float(best_score):
                    best_score = score
                    best = (connector_1, connector_2, TradeType.BUY, trade_profit, rate_1, rate_2, price_1, price_2, fee_1, fee_2, i_price_diff)

        return best_score, best
    
    def enough_balance(self, connector_name):
        connector = self.connectors[connector_name]
        avail_usd = float(connector.available_balances.get(self.quote_markets_map.get(connector_name, 'USDT'), 0))
        return avail_usd >= float(self.config.position_size_quote) / float(self.config.leverage), avail_usd

    def heuristic_profitability_evaluation(self, price_1, price_2, fee_1, fee_2, rate_1, rate_2, time_to_funding, i_price_diff):
        i_price_diff = max(0, i_price_diff)
        price_profit = (price_2 - price_1 - i_price_diff) / price_1 - self.config.min_price_diff
        funding_rate_profit = rate_2 - rate_1
        profit_rate = (price_profit + funding_rate_profit - fee_1 * 2 - fee_2 * 2) / time_to_funding
        return profit_rate

    def _funding_intervals_match(self, funding_info_report: Dict, connector_1: str, connector_2: str) -> bool:
        interval_1 = getattr(funding_info_report[connector_1], "funding_interval", None) \
            or self.funding_payment_interval_map.get(connector_1)
        interval_2 = getattr(funding_info_report[connector_2], "funding_interval", None) \
            or self.funding_payment_interval_map.get(connector_2)
        if interval_1 is None or interval_2 is None:
            # If either side lacks data, be conservative and reject pairing
            return False
        try:
            return abs(int(interval_1) - int(interval_2)) <= 60  # allow 1-minute wiggle
        except Exception:
            return False

    def good_time_to_trade(self):
        cur_min = self.current_timestamp / 60 % 60
        return cur_min >= 5 and cur_min <= 55

    # ------------------------
    # Consecutive-confirm helpers
    # ------------------------
    def _get_cond_deque(self, token: str):
        try:
            n = max(1, int(getattr(self.config, "condition_consecutive_required", 1)))
        except Exception:
            n = 1
        dq = self._cond_hits_map.get(token)
        if dq is None or (getattr(dq, "maxlen", None) != n):
            dq = deque(maxlen=n)
            self._cond_hits_map[token] = dq
        return dq

    def _note_condition_hit(self, token: str, now_sec: int) -> None:
        dq = self._get_cond_deque(token)
        if len(dq) == 0 or dq[-1] != now_sec:
            dq.append(now_sec)
        self.logger().debug(f"[consec] cond_hit token={token} now={now_sec} hits={list(dq)}")

    def _has_recent_consecutive_hits(self, token: str, now_sec: int) -> bool:
        dq = self._get_cond_deque(token)
        n = dq.maxlen or 1
        if len(dq) < n:
            return False
        for idx in range(n):
            if dq[-n + idx] != now_sec - (n - 1 - idx):
                return False
        self.logger().info(f"[consec] consecutive_check_pass token={token} now={now_sec}")
        return True

    # Stop-side helpers
    def _get_stop_cond_deque(self, token: str):
        try:
            n = max(1, int(getattr(self.config, "condition_consecutive_required", 1)))
        except Exception:
            n = 1
        dq = self._stop_cond_hits_map.get(token)
        if dq is None or (getattr(dq, "maxlen", None) != n):
            dq = deque(maxlen=n)
            self._stop_cond_hits_map[token] = dq
        return dq

    def _note_stop_condition_hit(self, token: str, now_sec: int) -> None:
        dq = self._get_stop_cond_deque(token)
        if len(dq) == 0 or dq[-1] != now_sec:
            dq.append(now_sec)
        self.logger().debug(f"[consec] stop_cond_hit token={token} now={now_sec} hits={list(dq)}")

    def _has_recent_consecutive_stop_hits(self, token: str, now_sec: int) -> bool:
        dq = self._get_stop_cond_deque(token)
        n = dq.maxlen or 1
        if len(dq) < n:
            return False
        for idx in range(n):
            if dq[-n + idx] != now_sec - (n - 1 - idx):
                return False
        self.logger().info(f"[consec] stop_consecutive_check_pass token={token} now={now_sec}")
        return True

    # ------------------------
    # Memory guard (lightweight, always on if limit set)
    # ------------------------
    def _current_rss_bytes(self) -> int:
        if _HAS_PSUTIL:
            try:
                return psutil.Process().memory_info().rss
            except Exception:
                pass
        try:
            with open("/proc/self/status", "r") as f:
                text = f.read()
            m = re.search(r"VmRSS:\\s+(\\d+)\\s+kB", text)
            if m:
                return int(m.group(1)) * 1024
        except Exception:
            pass
        return 0

    def _memory_guard_tick(self) -> None:
        try:
            limit_mb = int(getattr(self.config, "memory_guard_limit_mb", 0) or 0)
            if limit_mb <= 0:
                return
            interval = max(5, int(getattr(self.config, "memory_guard_check_interval_sec", 30)))
            now = time.time()
            if now - self._last_mem_guard_check < interval:
                return
            self._last_mem_guard_check = now
            rss = self._current_rss_bytes()
            if rss <= 0:
                return
            limit_bytes = limit_mb * 1024 * 1024
            if rss >= limit_bytes:
                if not self._mem_guard_triggered:
                    self.logger().warning(
                        f"[mem-guard] RSS {rss/(1024*1024):.1f} MB >= {limit_mb} MB, pausing new entries and dumping snapshot")
                self._mem_guard_triggered = True
                try:
                    gc.collect()
                except Exception:
                    pass
                try:
                    if self._mem_monitor is not None:
                        self._mem_monitor._begin_tracemalloc_session(300)
                        self._mem_monitor._dump_snapshot(reason="guard_rss")
                except Exception as e:
                    self.logger().debug(f"[mem-guard] snapshot failed: {e}")
            elif self._mem_guard_triggered and rss < limit_bytes * 0.9:
                self._mem_guard_triggered = False
                self.logger().info(
                    f"[mem-guard] RSS recovered to {rss/(1024*1024):.1f} MB (<{limit_mb} MB), resuming creation")
        except Exception as e:
            self.logger().debug(f"[mem-guard] check failed: {e}")

    def create_actions_proposal(self) -> List[CreateExecutorAction]:
        """
        In this method we are going to evaluate if a new set of positions has to be created for each of the tokens that
        don't have an active arbitrage.
        More filters can be applied to limit the creation of the positions, since the current logic is only checking for
        positive pnl between funding rate. Is logged and computed the trading profitability at the time for entering
        at market to open the possibilities for other people to create variations like sending limit position executors
        and if one gets filled buy market the other one to improve the entry prices.
        """
        self._memory_guard_tick()
        self.create_action_cool_down -= 1
        if self.create_action_cool_down > 0:
            self.logger().debug(f"Create action cool down: {self.create_action_cool_down}")
            return []
        # 1) 为每个 token 计算启发式评分最高的组合
        token_rankings = []
        for token in self.tokens_for_trading():
            if token in self.active_funding_arbitrages:
                continue
            self.token_failure_cool_down.setdefault(token, 0)
            self.token_failure_cool_down[token] -= 1
            if self.token_failure_cool_down[token] > 0:
                continue
            prices_and_fees_cache = {}
            funding_info_report = self.get_funding_info_by_token(token)
            score, best_combo = self.get_best_combination_by_heuristic(prices_and_fees_cache, funding_info_report, token)
            if best_combo is None:
                continue
            token_rankings.append((token, score, best_combo))

        # 2) 按评分从大到小排序
        token_rankings.sort(key=lambda x: float(x[1]), reverse=True)

        # 3) 逐个尝试原有阈值逻辑，符合则开仓并返回
        for token, _, best_combination in token_rankings:
            connector_1, connector_2, trade_side, expected_profitability, \
                rate_1, rate_2, price_1, price_2, fee_1, fee_2, i_price_diff = best_combination

            open_condition = expected_profitability >= self.config.min_trade_profitability \
                and rate_2 - rate_1 >= self.config.min_funding_profitability \
                and (price_2 - price_1 - i_price_diff) / price_1 >= self.config.min_price_diff
            
            if not open_condition:
                continue
            
            # balance check
            enough_1, balance_1 = self.enough_balance(connector_1)
            enough_2, balance_2 = self.enough_balance(connector_2)
            if not enough_1 or not enough_2:
                self.logger().warning(
                    f"Balance Not enough for {connector_1 if not enough_1 else connector_2} "
                    f"({(balance_1 if not enough_1 else balance_2):.3f}), didn't open positions")
                now_ts = self.current_timestamp
                if now_ts - self._last_insufficient_balance_alert_ts >= 1800:
                    try:
                        telegram_message = "**Balance Alert**\n"
                        telegram_message += "```\n"
                        telegram_message += f"Token     : {token}\n"
                        telegram_message += f"Conn 1    : {connector_1} bal={balance_1:.3f}\n"
                        telegram_message += f"Conn 2    : {connector_2} bal={balance_2:.3f}\n"
                        telegram_message += f"Needed    : {float(self.config.position_size_quote) / float(self.config.leverage):.3f}\n"
                        telegram_message += f"Time      : {self.format_utc(now_ts)}\n"
                        telegram_message += "```\n"
                        self._send_telegram(telegram_message)
                    except Exception as e:
                        self.logger().debug(f"Balance alert tg failed: {e}")
                    self._last_insufficient_balance_alert_ts = now_ts
                continue

            if self.is_stopping_creating_actions:
                self.logger().debug(
                    f"Stopping creating actions, skipping creation of executors for {token}")
                continue

            if self._mem_guard_triggered:
                self.logger().debug(
                    f"[mem-guard] Mem guard triggered, skipping creation of executors for {token}")
                continue
                
            if not self.good_time_to_trade():
                self.logger().debug(f"[good_time_to_trade] Not good time to trade, skipping creation of executors for {token}")
                continue

            # 连续确认：记录本秒达标并检查是否满足最近N秒连续达标
            now_sec = int(self.current_timestamp)
            self._note_condition_hit(token, now_sec)
            if not self._has_recent_consecutive_hits(token, now_sec):
                self.logger().debug(
                    f"[consec] consecutive_check_fail token={token} now={now_sec} N={getattr(self.config, 'condition_consecutive_required', 1)}")
                continue

            self.logger().info("Starting executors...")

            self.logger().info(
                f"Best Combination: {token} | {connector_1} | {connector_2} | {trade_side} | "
                    f"rate_1={self.format_percent(rate_1)} | rate_2={self.format_percent(rate_2)} | "
                    f"price_1={price_1:.7f} | price_2={price_2:.7f} | "
                    f"i_price_diff={i_price_diff:.7f} | "
                    f"i_price_diff_pct={self.format_percent(i_price_diff/price_1)} | "
                    f"fee_1={self.format_percent(fee_1)} | fee_2={self.format_percent(fee_2)} | "
                    f"balance_1={balance_1:.3f} | balance_2={balance_2:.3f} | "
                    f"expected_profitability={self.format_percent(expected_profitability)} ")

            if i_price_diff / price_1 > 0.01:
                self.logger().debug("Abort creating actions because huge index price diff")
                continue

            position_executor_config_1, position_executor_config_2 = \
                self.get_position_executors_config(token, connector_1, connector_2, trade_side, price_1, price_2)
            self.active_funding_arbitrages[token] = {
                "connector_1": connector_1,
                "connector_2": connector_2,
                "rate_1": rate_1,
                "rate_2": rate_2,
                "price_1": price_1,
                "price_2": price_2,
                "i_price_diff": i_price_diff,
                "fee_1": fee_1,
                "fee_2": fee_2,
                "expected_profitability": expected_profitability,
                "executors_ids": [position_executor_config_1.id, position_executor_config_2.id],
                "side": trade_side,
                "funding_payments": [],
                "start_time": self.current_timestamp,
                "position_prices": {
                    connector_1: price_1,
                    connector_2: price_2,
                },
            }
            self.create_action_cool_down = CREATE_ACTION_COOL_DOWN_COUNT
            return [CreateExecutorAction(executor_config=position_executor_config_1),
                    CreateExecutorAction(executor_config=position_executor_config_2)]

        return []

    def create_stop_executor_action(self, executors: List[ExecutorInfo], \
                                    price_1: float = None, price_2: float = None) -> List[StopExecutorAction]:
        stop_actions = []
        for executor in executors:
            if executor.custom_info["side"] == TradeType.BUY:
                stop_actions.append(StopExecutorAction(executor_id=executor.id, \
                                                       stop_config={'expect_close_price':price_1}))
            elif executor.custom_info["side"] == TradeType.SELL:
                stop_actions.append(StopExecutorAction(executor_id=executor.id, \
                                                       stop_config={'expect_close_price':price_2}))
        return stop_actions
    
    def get_executors(self, executor_ids):
        return self.filter_executors(
            executors=self.get_all_executors(),
            filter_func=lambda x: x.id in executor_ids
        )

    def check_is_liquidated(self, connector_1, connector_2, token):
        def get_position(connector, token, side):
            pos_key = f"{token}-{self.quote_markets_map.get(connector, 'USDT')}{side}"
            if connector == 'bybit_perpetual':
                for pos_key_ in [pos_key + str(i) for i in range(3)]:
                    position = self.connectors[connector].account_positions.get(pos_key_, None)
                    if position is not None:
                        return position
                return None
            if connector == 'gate_io_perpetual':
                pos_key = f"{token}-{self.quote_markets_map.get(connector, 'USDT')}"
            return self.connectors[connector].account_positions.get(pos_key, None)
        position_1 = get_position(connector_1, token, "LONG")
        position_2 = get_position(connector_2, token, "SHORT")
        if position_1 is None:
            self.logger().debug(f"Position not found for {token} {connector_1}")
            return True
        if position_2 is None:
            self.logger().debug(f"Position not found for {token} {connector_2}")
            return True
        if position_1.amount == Decimal(0) or position_2.amount == Decimal(0):
            self.logger().debug(f"Position amount is 0 for {token} {connector_1} {connector_2}, position_1: {repr(position_1)}, position_2: {repr(position_2)}")
            return True
        return False

    def _update_position_prices(self, token: str, connector_prices: Dict[str, Decimal]) -> None:
        try:
            if token not in self.active_funding_arbitrages:
                return
            self.active_funding_arbitrages[token].setdefault("position_prices", {})
            self.active_funding_arbitrages[token]["position_prices"].update(connector_prices)
        except Exception as e:
            self.logger().debug(f"Failed to update position prices for {token}: {e}")

    def _liquidation_distance_pct(self, executor: ExecutorInfo, current_price: Decimal):
        try:
            entry_price_raw = executor.custom_info.get("actual_open_price") or executor.custom_info.get("entry_price")
            side = executor.custom_info.get("side")
            if entry_price_raw in (None, 0) or side not in (TradeType.BUY, TradeType.SELL):
                return None
            entry_price = Decimal(str(entry_price_raw))
            leverage_raw = executor.custom_info.get("leverage") or self.config.leverage
            leverage = Decimal(str(leverage_raw))
            if leverage <= 0:
                return None

            if side == TradeType.BUY:
                liquidation_price = entry_price * (Decimal(1) - Decimal(1) / leverage)
                if liquidation_price <= 0:
                    return None
                return (current_price - liquidation_price) / liquidation_price
            liquidation_price = entry_price * (Decimal(1) + Decimal(1) / leverage)
            return (liquidation_price - current_price) / liquidation_price
        except Exception as e:
            self.logger().debug(f"Failed to compute liquidation distance for executor {executor.id}: {e}")
            return None

    def stop_actions_proposal(self) -> List[StopExecutorAction]:
        """
        Once the funding rate arbitrage is created we are going to control the funding payments pnl and the current
        pnl of each of the executors at the cost of closing the open position at market.
        If that PNL is greater than the profitability_to_take_profit
        """
        self._memory_guard_tick()
        stop_executor_actions = []
        stopped_tokens = []
        for token, funding_arbitrage_info in self.active_funding_arbitrages.items():
            executors = self.get_executors(funding_arbitrage_info["executors_ids"])
            closed_ex = list(ex.close_type for ex in executors if ex.close_type)
            if len(closed_ex) > 0:
                self.logger().warning(f"Closed executor for {token} found due to "
                                      f"{','.join([close.name for close in closed_ex])}, stopping executors")
                self.token_failure_cool_down[token] = TOKEN_FAILURE_COOL_DOWN_COUNT
                stopped_tokens.append(token)
                funding_arbitrage_info['stop_reason'] = "UNK"
                funding_arbitrage_info['stop_time'] = self.current_timestamp
                self.stopped_funding_arbitrages[token].append(funding_arbitrage_info)
                stop_executor_actions.extend(self.create_stop_executor_action(executors))
                continue
            connector_1 = funding_arbitrage_info["connector_1"]
            connector_2 = funding_arbitrage_info["connector_2"]
            if self.current_timestamp - funding_arbitrage_info['start_time'] > 60 and \
                        self.check_is_liquidated(connector_1, connector_2, token):
                self.logger().debug(f"Liquidation detected for {token}, stopping executors")
                stopped_tokens.append(token)
                funding_arbitrage_info['stop_reason'] = "LIQ"
                funding_arbitrage_info['stop_time'] = self.current_timestamp
                self.stopped_funding_arbitrages[token].append(funding_arbitrage_info)
                stop_executor_actions.extend(self.create_stop_executor_action(executors))
                continue
            # pre-liquidation check
            if len(executors) != 2:
                self.logger().debug(f"Executors not found for {token} ({len(executors)}) when stop actions proposal")
                continue
            try:
                max_hold_seconds = int(getattr(self.config, "max_hold_time_minutes", 0)) * 60
            except Exception:
                max_hold_seconds = 0
            if max_hold_seconds > 0 and self.current_timestamp - funding_arbitrage_info.get("start_time", 0) >= max_hold_seconds:
                self.logger().info(f"Max hold time reached for {token}, stopping executors")
                stopped_tokens.append(token)
                funding_arbitrage_info['stop_reason'] = "TIME"
                funding_arbitrage_info['stop_time'] = self.current_timestamp
                self.stopped_funding_arbitrages[token].append(funding_arbitrage_info)
                stop_executor_actions.extend(self.create_stop_executor_action(executors))
                continue
            funding_payments_pnl = sum(funding_payment.amount for funding_payment in funding_arbitrage_info["funding_payments"])
            funding_payments_pnl_pct = funding_payments_pnl / self.config.position_size_quote
            fee_1, fee_2 = funding_arbitrage_info["fee_1"], funding_arbitrage_info["fee_2"]
            a_price_1 = executors[0].custom_info['actual_open_price']
            a_price_2 = executors[1].custom_info['actual_open_price']
            if not a_price_1 or not a_price_2:
                self.logger().debug(f"Skip stop actions judgement {token} because open-order didn't filled")
                continue
            c_price_1, _ = self.get_price_and_fee_with_cache( \
                {}, connector_1, token, TradeType.SELL)
            c_price_2, _ = self.get_price_and_fee_with_cache( \
                {}, connector_2, token, TradeType.BUY)
            self._update_position_prices(token, {
                connector_1: c_price_1,
                connector_2: c_price_2,
            })
            executor_1, executor_2 = executors

            try:
                liquidation_buffer_pct = Decimal(str(getattr(self.config, "liquidation_buffer_pct", Decimal("0.05"))))
            except Exception:
                liquidation_buffer_pct = Decimal("0")
            if liquidation_buffer_pct < 0:
                liquidation_buffer_pct = Decimal("0")

            liq_distance_1 = self._liquidation_distance_pct(executor_1, c_price_1)
            liq_distance_2 = self._liquidation_distance_pct(executor_2, c_price_2)
            if (liq_distance_1 is not None and liq_distance_1 <= liquidation_buffer_pct) or \
               (liq_distance_2 is not None and liq_distance_2 <= liquidation_buffer_pct):
                self.logger().warning(
                    f"Liquidation proximity detected for {token}: "
                    f"{connector_1} dist={liq_distance_1 if liq_distance_1 is not None else 'N/A'}, "
                    f"{connector_2} dist={liq_distance_2 if liq_distance_2 is not None else 'N/A'}")
                stopped_tokens.append(token)
                funding_arbitrage_info['stop_reason'] = "LIQ_NEAR"
                funding_arbitrage_info['stop_time'] = self.current_timestamp
                self.stopped_funding_arbitrages[token].append(funding_arbitrage_info)
                stop_executor_actions.extend(self.create_stop_executor_action(executors, c_price_1, c_price_2))
                continue

            executors_pnl = sum(executor.net_pnl_pct for executor in executors)
            price_1 = funding_arbitrage_info['price_1']
            executors_pnl_by_hand = (a_price_2 - a_price_1 - c_price_2 + c_price_1) / price_1 - fee_1 - fee_2
            # self.logger().debug(f"{token} executors_pnl={executors_pnl:.4%}, by_hand={executors_pnl_by_hand:.4%}")
            executors_trade_pnl = sum(executor.custom_info['trade_pnl_pct'] for executor in executors)
            trade_pnl_by_had = (a_price_2 - a_price_1 - c_price_2 + c_price_1) / price_1
            # self.logger().debug(f"{executors_trade_pnl=:.4%}, by_hand={trade_pnl_by_had:.4%}")
            # self.logger().debug(f"{a_price_1=:.7f},{a_price_2=:.7f},{c_price_1=:.7f},{c_price_2=:.7f}")
            # self.logger().debug(f"{executor_1.custom_info['entry_price']=:.7f},{executor_2.custom_info['entry_price']=:.7f}")
            # self.logger().debug(f"{executor_1.custom_info['close_price']=:.7f},{executor_2.custom_info['close_price']=:.7f}")
            funding_info_report = self.get_funding_info_by_token(token)
            rate_1 = funding_info_report[connector_1].rate
            rate_2 = funding_info_report[connector_2].rate

            take_profit_condition = executors_pnl_by_hand + funding_payments_pnl_pct > \
                                    self.config.min_take_profit + fee_1 + fee_2
            keep_holding_condition = rate_2 - rate_1 > trade_pnl_by_had and rate_2 - rate_1 > 0
            keep_holding_condition = keep_holding_condition or (self.config.min_funding_profitability > 0 and len(funding_arbitrage_info["funding_payments"]) < 2)
            if take_profit_condition and keep_holding_condition:
                self.logger().info("TP reached but holding")
            # do not use keep_holding_condition for now
            # take_profit_condition = take_profit_condition and not keep_holding_condition

            # do not use realtime index price diff for stop loss
            # i_price_diff = funding_info_report[connector_2].index_price - funding_info_report[connector_1].index_price
            i_price_diff = funding_arbitrage_info['i_price_diff']

            # TODO strengthen stop_loss_condition
            stop_loss_condition = False
            stop_loss_type = None
            if len(funding_arbitrage_info["funding_payments"]) >= 2:
                rate_diff = rate_2 - rate_1
                price_diff = (c_price_2 - c_price_1 - i_price_diff) / c_price_1
                profitability = rate_diff + price_diff - fee_1 - fee_2
                if price_diff < 0 and profitability < self.config.min_take_profit:
                    stop_loss_condition = True
                    stop_loss_type = "1"
                elif rate_diff < 0 and profitability < self.config.min_take_profit:
                    stop_loss_condition = True
                    stop_loss_type = "2"
                elif price_diff < self.config.min_price_diff and rate_diff < self.config.min_funding_profitability:
                    stop_loss_condition = True
                    stop_loss_type = "3"

            stop_condition_now = take_profit_condition or stop_loss_condition
            if stop_condition_now and not self.good_time_to_trade():
                self.logger().debug(f"[good_time_to_trade] Not good time to trade, skipping stop of executors for {token}")
                continue

            # 连续确认：若满足任一止盈/止损条件，则记录本秒命中并检查是否连续N秒
            if stop_condition_now:
                now_sec = int(self.current_timestamp)
                self._note_stop_condition_hit(token, now_sec)
                if not self._has_recent_consecutive_stop_hits(token, now_sec):
                    self.logger().debug(
                        f"[consec] stop_consecutive_check_fail token={token} now={now_sec} N={getattr(self.config, 'condition_consecutive_required', 1)}")
                    continue

            if take_profit_condition:
                self.logger().info(f"Take profit profitability reached for {token}, stopping executors, "
                                   f"{executors_pnl_by_hand=:.4%}, "
                                   f"{funding_payments_pnl_pct=:.4%}")
                stopped_tokens.append(token)
                funding_arbitrage_info['stop_reason'] = "TP"
                funding_arbitrage_info['stop_time'] = self.current_timestamp
                self.stopped_funding_arbitrages[token].append(funding_arbitrage_info)
                stop_executor_actions.extend(self.create_stop_executor_action(executors, c_price_1, c_price_2))
            elif stop_loss_condition:
                self.logger().info(f"Stop loss condition satisfied for {token}, stopping executors")
                stopped_tokens.append(token)
                funding_arbitrage_info['stop_reason'] = f"SL-{stop_loss_type}"
                funding_arbitrage_info['stop_time'] = self.current_timestamp
                self.stopped_funding_arbitrages[token].append(funding_arbitrage_info)
                stop_executor_actions.extend(self.create_stop_executor_action(executors, c_price_1, c_price_2))
        for token in stopped_tokens:
            self.active_funding_arbitrages.pop(token, None)
        return stop_executor_actions

    def did_complete_funding_payment(self, funding_payment_completed_event: FundingPaymentCompletedEvent):
        """
        Based on the funding payment event received, check if one of the active arbitrages matches to add the event
        to the list.
        """
        token = funding_payment_completed_event.trading_pair.split("-")[0]
        market = funding_payment_completed_event.market
        amount = funding_payment_completed_event.amount
        if token in self.active_funding_arbitrages:
            self.logger().info(f"Funding payment collected for {token} by {market}, amount = {float(amount):.3f} USD")
            self.active_funding_arbitrages[token]["funding_payments"].append(funding_payment_completed_event)

    def get_position_executors_config(self, token, connector_1, connector_2, trade_side, price_1, price_2):
        position_amount = self.config.position_size_quote / price_1
        create_executor_time = time.time()
        position_executor_config_1 = PositionExecutorConfig(
            timestamp=create_executor_time,
            connector_name=connector_1,
            trading_pair=self.get_trading_pair_for_connector(token, connector_1),
            side=trade_side,
            amount=position_amount,
            entry_price=price_1,
            leverage=self.config.leverage,
            triple_barrier_config=TripleBarrierConfig(open_order_type=OrderType.MARKET),
        )
        position_executor_config_2 = PositionExecutorConfig(
            timestamp=create_executor_time,
            connector_name=connector_2,
            trading_pair=self.get_trading_pair_for_connector(token, connector_2),
            side=TradeType.BUY if trade_side == TradeType.SELL else TradeType.SELL,
            amount=position_amount,
            entry_price=price_2,
            leverage=self.config.leverage,
            triple_barrier_config=TripleBarrierConfig(open_order_type=OrderType.MARKET),
        )
        return position_executor_config_1, position_executor_config_2

    def format_percent(self, x) -> str:
        return f"{x:>7.3%}"
    
    def format_currency(self, x) -> str:
        return f"{x:>7.3f}"
    
    def format_time(self, x) -> str:
        sign = '' if x > 0 else '-'
        x = abs(x)
        hours, remainder = divmod(x, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        return f"{sign}{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"

    def format_utc(self, x) -> str:
        utc_time = datetime.utcfromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S UTC')
        return utc_time

    # ------------------------
    # Telegram helpers (optional)
    # ------------------------
    def _send_telegram(self, text: str) -> None:
        import urllib.request
        import urllib.parse
        import urllib.error
        # Telegram 单条消息最大 4096 字符，必要时截断
        def _maybe_truncate(msg: str) -> str:
            return msg if len(msg) <= 4096 else (msg[:4060] + "\n...[truncated]...")
        try:
            token = (getattr(self.config, "telegram_bot_token", "") or os.getenv("TG_BOT_TOKEN", "")).strip()
            chat_id = (getattr(self.config, "telegram_chat_id", "") or os.getenv("TG_CHAT_ID", "")).strip()
            if not token or not chat_id:
                return
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            headers = {"Content-Type": "application/x-www-form-urlencoded"}
            # 优先尝试 MarkdownV2，若解析失败再回退为纯文本
            payload = {"chat_id": chat_id, "text": _maybe_truncate(text), "parse_mode": "MarkdownV2"}
            data = urllib.parse.urlencode(payload).encode()
            req = urllib.request.Request(url, data=data, headers=headers)
            try:
                resp_bytes = urllib.request.urlopen(req, timeout=5).read()
                self.logger().debug(f"[tg] sent successfully: {resp_bytes!r}")
                return
            except urllib.error.HTTPError as he:
                # 读取错误响应体，便于诊断（例如 can't parse entities、chat not found 等）
                body = b""
                try:
                    body = he.read()
                except Exception:
                    pass
                try:
                    body_text = body.decode("utf-8", "ignore")
                except Exception:
                    body_text = repr(body)
                self.logger().debug(f"[tg] HTTPError {he.code}: {he.reason}. body={body_text}")
                # 如果是 Markdown 解析错误，回退为纯文本再试一次
                if "parse" in body_text.lower() or "can't parse" in body_text.lower():
                    try:
                        payload_fb = {"chat_id": chat_id, "text": _maybe_truncate(text)}
                        data_fb = urllib.parse.urlencode(payload_fb).encode()
                        req_fb = urllib.request.Request(url, data=data_fb, headers=headers)
                        resp_bytes_fb = urllib.request.urlopen(req_fb, timeout=5).read()
                        self.logger().debug(f"[tg] sent without parse_mode: {resp_bytes_fb!r}")
                        return
                    except Exception as e2:
                        self.logger().debug(f"[tg] fallback send failed: {e2}")
                        return
                return
        except Exception as e:
            self.logger().debug(f"[tg] send failed: {e}")
            return

    def _maybe_send_daily_pnl_reminder(self):
        """
        Send a daily PnL reminder if:
        - no active arbitrage
        - more than 24h since last reminder
        """
        now_ts = self.current_timestamp
        if len(self.active_funding_arbitrages) > 0:
            return
        if self._last_daily_reminder_ts == 0:
            # initialize baseline without sending
            self._last_daily_reminder_ts = now_ts
            self._last_daily_total_balance = self._get_total_balance_value()
            self._last_daily_stopped_counts = {token: len(self.stopped_funding_arbitrages.get(token, []))
                                               for token in self.stopped_funding_arbitrages}
            return
        if now_ts - self._last_daily_reminder_ts < 24 * 3600:
            return
        cur_total = self._get_total_balance_value()
        prev_total = self._last_daily_total_balance if self._last_daily_total_balance is not None else cur_total
        pnl = cur_total - prev_total
        total_stopped = {token: len(self.stopped_funding_arbitrages.get(token, []))
                         for token in self.stopped_funding_arbitrages}
        daily_count = sum(total_stopped.get(t, 0) - self._last_daily_stopped_counts.get(t, 0) for t in total_stopped)
        tp_count = 0
        for token, items in self.stopped_funding_arbitrages.items():
            start_idx = self._last_daily_stopped_counts.get(token, 0)
            for item in items[start_idx:]:
                if item.get("stop_reason") == "TP":
                    tp_count += 1

        telegram_message = "**Daily PnL Summary**\n"
        telegram_message += "```\n"
        telegram_message += f"PnL (USD): {pnl:.3f}\n"
        telegram_message += f"Arb Count: {daily_count}\n"
        telegram_message += f"TP Count : {tp_count}\n"
        telegram_message += f"Total Bal: {cur_total:.3f}\n"
        telegram_message += f"Time     : {self.format_utc(now_ts)}\n"
        telegram_message += "```\n"
        self._send_telegram(telegram_message)

        self._last_daily_reminder_ts = now_ts
        self._last_daily_total_balance = cur_total
        self._last_daily_stopped_counts = total_stopped

    def get_balances_info(self):
        balances_info = [{ "USDT": "Avail", "All": 0 }, { "USDT": "Total", "All": 0 }]
        for connector_name in self.connectors.keys():
            avail_usd = self.connectors[connector_name].available_balances.get(self.quote_markets_map.get(connector_name, 'USDT'), 0)
            total_usd = self.connectors[connector_name].get_balance(self.quote_markets_map.get(connector_name, 'USDT'))
            avail_usd = float(avail_usd)
            total_usd = float(total_usd)
            balances_info[0][connector_name.replace("_perpetual", "")] = avail_usd
            balances_info[1][connector_name.replace("_perpetual", "")] = total_usd
            balances_info[0]["All"] += avail_usd
            balances_info[1]["All"] += total_usd
        for item in balances_info:
            for key in item.keys():
                if key != "USDT":
                    item[key] = self.format_currency(item[key])
        return balances_info

    def _get_total_balance_value(self) -> float:
        total = 0.0
        for connector_name in self.connectors.keys():
            total += float(self.connectors[connector_name].get_balance(self.quote_markets_map.get(connector_name, 'USDT')))
        return total

    def format_status(self) -> str:
        original_status = super().format_status()
        funding_rate_status = []
        if self.ready_to_trade:
            all_funding_info = [ {"Connector": connector_name} for connector_name in self.connectors.keys() ]
            all_best_paths = []
            for token in self.tokens_for_trading():
                
                funding_info_report = self.get_funding_info_by_token(token)
                for connector_name, info in funding_info_report.items():
                    for funding_info in all_funding_info:
                        if funding_info["Connector"] == connector_name:
                            funding_info[token] = self.format_percent(info.rate)
                            break

                best_paths_info = {"Token": token}
                prices_and_fees_cache = dict()
                best_combination = self.get_most_trade_profitable_combination(prices_and_fees_cache, \
                                                                               funding_info_report, token, funding_time_check=False)
                if best_combination:
                    connector_1, connector_2, trade_side, expected_profitability, \
                        rate_1, rate_2, price_1, price_2, fee_1, fee_2, i_price_diff = best_combination
                    best_paths_info["Best Path"] = f"{connector_1}_{connector_2}"
                    best_paths_info["Pirce Diff"] = self.format_percent((price_2 - price_1) / price_1)
                    best_paths_info["Index Diff"] = self.format_percent(i_price_diff / price_1)
                    best_paths_info["Rate Diff"] = self.format_percent((rate_2 - rate_1))
                    best_paths_info["Fees"] = self.format_percent((fee_1 + fee_2))
                    best_paths_info["Trade Profit"] = self.format_percent(expected_profitability)

                    time_to_next_funding_info_c1 = funding_info_report[connector_1].next_funding_utc_timestamp - self.current_timestamp
                    time_to_next_funding_info_c2 = funding_info_report[connector_2].next_funding_utc_timestamp - self.current_timestamp
                    best_paths_info["Time to Funding 1"] = self.format_time(time_to_next_funding_info_c1)
                    best_paths_info["Time to Funding 2"] = self.format_time(time_to_next_funding_info_c2)
                    all_best_paths.append(best_paths_info)

            funding_rate_status.append(f"\nMin Trade Profitability: {self.config.min_trade_profitability:.2%}")

            funding_rate_status.append("Funding Rate Info")
            funding_rate_status.append(format_df_for_printout(df=pd.DataFrame(all_funding_info), table_format="psql",))
            funding_rate_status.append(format_df_for_printout(df=pd.DataFrame(all_best_paths), table_format="psql",))
            funding_rate_status.append("USDT Balances")
            funding_rate_status.append(format_df_for_printout(df=pd.DataFrame(self.get_balances_info()), table_format="psql"))

            funding_rate_status.append(f"\nActive Funding Arbitrages:")
            active_arbitrage_info = []
            active_arbitrage_debug = []
            for token, funding_arbitrage_info in self.active_funding_arbitrages.items():
                arbitrage_info = { "Token": token }
                arbitrage_info["Connector 1"] = funding_arbitrage_info["connector_1"]
                arbitrage_info["Connector 2"] = funding_arbitrage_info["connector_2"]
                funding_payments_pnl = \
                    sum(funding_payment.amount for funding_payment in funding_arbitrage_info["funding_payments"]) \
                    / self.config.position_size_quote
                price_1, price_2 = funding_arbitrage_info["price_1"], funding_arbitrage_info["price_2"]
                rate_1, rate_2 = funding_arbitrage_info["rate_1"], funding_arbitrage_info["rate_2"]
                fee_1, fee_2 = funding_arbitrage_info["fee_1"], funding_arbitrage_info["fee_2"]
                i_price_diff = funding_arbitrage_info["i_price_diff"]
                arbitrage_info["Px Diff"] = self.format_percent((price_2 - price_1) / price_1)
                arbitrage_info["Ix Diff"] = self.format_percent(i_price_diff / price_1)
                arbitrage_info["Fd Diff"] = self.format_percent(rate_2 - rate_1)
                # arbitrage_info["Fee1+Fee2"] = self.format_percent(fee_1 + fee_2)
                c_price_1, _ = self.get_price_and_fee_with_cache( \
                    {}, funding_arbitrage_info["connector_1"], token, TradeType.SELL)
                c_price_2, _ = self.get_price_and_fee_with_cache( \
                    {}, funding_arbitrage_info["connector_2"], token, TradeType.BUY)
                executors = self.get_executors(funding_arbitrage_info["executors_ids"])
                if len(executors) != 2:
                    continue
                executor_1, executor_2 = executors
                a_price_1 = executor_1.custom_info['actual_open_price']
                a_price_2 = executor_2.custom_info['actual_open_price']
                if not a_price_1 or not a_price_2:
                    self.logger().debug(f"Skip format_status {token} because open-order didn't filled")
                    continue
                arbitrage_info["Delay"] = f"{executor_1.custom_info['open_delay']*1e3:.1f}ms," \
                                          f"{executor_2.custom_info['open_delay']*1e3:.1f}ms"
                arbitrage_info["Sllipage"] = f"{executor_1.custom_info['open_sllipage']:.3%}," \
                                             f"{executor_2.custom_info['open_sllipage']:.3%}"
                arbitrage_info["Fd Pnl"] = self.format_percent(funding_payments_pnl)
                arbitrage_info["Px Diff(Tar)"] = self.format_percent((a_price_2 - a_price_1) / price_1 \
                    + funding_payments_pnl - 2 * fee_1 - 2 * fee_2 - self.config.min_trade_profitability)
                arbitrage_info["Px Diff(Cur)"] = self.format_percent((c_price_2 - c_price_1) / price_1)
                arbitrage_info["Hold Time"] = \
                    self.format_time(self.current_timestamp - funding_arbitrage_info["start_time"])
                active_arbitrage_info.append(arbitrage_info)

                arbitrage_debug = { "token": token }
                e_price_1 = executor_1.custom_info['expect_open_price']
                e_price_2 = executor_2.custom_info['expect_open_price']
                create_t1 = executor_1.custom_info['executor_create_timestamp']
                create_t2 = executor_2.custom_info['executor_create_timestamp']
                complete_t1 = executor_1.custom_info['open_order_complete_timestamp']
                complete_t2 = executor_2.custom_info['open_order_complete_timestamp']
                arbitrage_debug["e_price_1"] = f"{e_price_1:.7f}"
                arbitrage_debug["a_price_1"] = f"{a_price_1:.7f}"
                arbitrage_debug["create_t1"] = f"{divmod(create_t1,60)[1]:.4f}"
                arbitrage_debug["complete_t1"] = f"{divmod(complete_t1,60)[1]:.4f}"
                arbitrage_debug["e_price_2"] = f"{e_price_2:.7f}"
                arbitrage_debug["a_price_2"] = f"{a_price_2:.7f}"
                arbitrage_debug["create_t2"] = f"{divmod(create_t2,60)[1]:.4f}"
                arbitrage_debug["complete_t2"] = f"{divmod(complete_t2,60)[1]:.4f}"
                active_arbitrage_debug.append(arbitrage_debug)
            self.status_active_arbitrage_info = active_arbitrage_info
            funding_rate_status.append( \
                format_df_for_printout(df=pd.DataFrame(active_arbitrage_info), table_format="psql",))
            funding_rate_status.append( \
                format_df_for_printout(df=pd.DataFrame(active_arbitrage_debug), table_format="psql",))

            funding_rate_status.append(f"\nStopped Funding Arbitrages:")
            stopped_arbitrage_info = []
            for token, funding_arbitrage_infos in self.stopped_funding_arbitrages.items():
                for funding_arbitrage_info in funding_arbitrage_infos:
                    arbitrage_info = {'Token': token}
                    connector_1 = funding_arbitrage_info["connector_1"]
                    connector_2 = funding_arbitrage_info["connector_2"]
                    arbitrage_info['Conn 1'] = connector_1.replace('_perpetual', '')
                    arbitrage_info['Conn 2'] = connector_2.replace('_perpetual', '')
                    price_1, price_2 = funding_arbitrage_info["price_1"], funding_arbitrage_info["price_2"]
                    rate_1, rate_2 = funding_arbitrage_info["rate_1"], funding_arbitrage_info["rate_2"]
                    fee_1, fee_2 = funding_arbitrage_info["fee_1"], funding_arbitrage_info["fee_2"]
                    i_price_diff = funding_arbitrage_info["i_price_diff"]
                    arbitrage_info["Px Diff"] = self.format_percent((price_2 - price_1) / price_1)
                    arbitrage_info["Ix Diff"] = self.format_percent(i_price_diff / price_1)
                    arbitrage_info["Fd Diff"] = self.format_percent(rate_2 - rate_1)
                    executors = self.get_executors(funding_arbitrage_info["executors_ids"])
                    if len(executors) != 2:
                        continue
                    executor_1, executor_2 = executors
                    a_price_1 = executor_1.custom_info['actual_open_price']
                    a_price_2 = executor_2.custom_info['actual_open_price']
                    open_delay_1 = f"{executor_1.custom_info['open_delay']*1e3:.1f}ms" if a_price_1 else "None"
                    open_delay_2 = f"{executor_2.custom_info['open_delay']*1e3:.1f}ms" if a_price_2 else "None"
                    open_sllipage_1 = f"{executor_1.custom_info['open_sllipage']:.3%}" if a_price_1 else "None"
                    open_sllipage_2 = f"{executor_2.custom_info['open_sllipage']:.3%}" if a_price_2 else "None"
                    arbitrage_info['Open Delay'] = f"{open_delay_1},{open_delay_2}"
                    arbitrage_info['Open Sllipage'] = f"{open_sllipage_1},{open_sllipage_2}"

                    a_price_1 = executor_1.custom_info['actual_close_price']
                    a_price_2 = executor_2.custom_info['actual_close_price']
                    e_price_1 = executor_1.custom_info['expect_close_price']
                    e_price_2 = executor_2.custom_info['expect_close_price']
                    normal_closed_1 = e_price_1 and a_price_1
                    normal_closed_2 = e_price_2 and a_price_2
                    close_delay_1 = f"{executor_1.custom_info['close_delay']*1e3:.1f}ms" if normal_closed_1 else "None"
                    close_delay_2 = f"{executor_2.custom_info['close_delay']*1e3:.1f}ms" if normal_closed_2 else "None"
                    close_sllipage_1 = f"{executor_1.custom_info['close_sllipage']:.3%}" if normal_closed_1 else "None"
                    close_sllipage_2 = f"{executor_2.custom_info['close_sllipage']:.3%}" if normal_closed_2 else "None"
                    arbitrage_info['Close Delay'] = f"{close_delay_1},{close_delay_2}"
                    arbitrage_info['Close Sllipage'] = f"{close_sllipage_1},{close_sllipage_2}"

                    close_type_1 = executor_1.close_type.name if a_price_1 else "None"
                    close_type_2 = executor_2.close_type.name if a_price_2 else "None"
                    arbitrage_info['Close Type'] = f"{close_type_1},{close_type_2}"

                    funding_payments_pnl = \
                        sum(funding_payment.amount for funding_payment in funding_arbitrage_info["funding_payments"]) \
                        / self.config.position_size_quote
                    executors_pnl = sum(executor.net_pnl_pct for executor in executors)
                    closed_ex = list(ex.close_type for ex in executors if ex.close_type)
                    all_executors_closed = len(closed_ex) == len(executors)
                    arbitrage_info['Fd Pnl'] = self.format_percent(funding_payments_pnl)
                    arbitrage_info['Td Pnl'] = self.format_percent(executors_pnl)
                    arbitrage_info['SR'] = funding_arbitrage_info['stop_reason']
                    hold_time = self.format_time(funding_arbitrage_info['stop_time'] - funding_arbitrage_info['start_time'])
                    arbitrage_info['Hold Time'] = hold_time
                    stop_time = datetime.utcfromtimestamp(funding_arbitrage_info['stop_time']).strftime('%Y-%m-%d %H:%M:%S UTC')
                    arbitrage_info['Stop Time'] = stop_time
                    arbitrage_info['Ex Closed'] = all_executors_closed

                    stopped_arbitrage_info.append(arbitrage_info)
            self.status_stopped_arbitrage_info = stopped_arbitrage_info
            funding_rate_status.append( \
                format_df_for_printout(df=pd.DataFrame(stopped_arbitrage_info), table_format="psql",))
        return original_status + "\n".join(funding_rate_status)

    def _on_tokens_updated_cleanup(self, new_tokens: Set[str]) -> None:
        """
        在动态WS订阅变更后，清理与旧代币/旧交易对相关的缓存与状态，避免持有无用引用导致内存增长。
        """
        try:
            new_tokens_set = set(new_tokens)

            # 1) 移除已不在订阅集合中的活动套利条目（正常情况下执行动态扫描时应无活动仓位，这里兜底清理）
            for token in list(self.active_funding_arbitrages.keys()):
                if token not in new_tokens_set:
                    self.active_funding_arbitrages.pop(token, None)

            # 2) 清理支持交换所缓存映射
            for token in list(self.tokens_supported_exchange_map.keys()):
                if token not in new_tokens_set:
                    self.tokens_supported_exchange_map.pop(token, None)

            # 2.1) 清理连续确认命中队列
            for token in list(getattr(self, "_cond_hits_map", {}).keys()):
                if token not in new_tokens_set:
                    self._cond_hits_map.pop(token, None)
            for token in list(getattr(self, "_stop_cond_hits_map", {}).keys()):
                if token not in new_tokens_set:
                    self._stop_cond_hits_map.pop(token, None)

            # 3) 仅保留新代币的失败冷却计数
            self.token_failure_cool_down = {t: self.token_failure_cool_down.get(t, 0) for t in new_tokens_set}

            # 4) EMA 价格缓存与TopK调试缓存清空（交易对已整体调整，保留无意义）
            self._ema_prices.clear()
            self._latest_topk_debug = []

            # 5) 为新代币确保有stopped结构，避免后续引用KeyError
            for t in new_tokens_set:
                if t not in self.stopped_funding_arbitrages:
                    self.stopped_funding_arbitrages[t] = []

            self.logger().info(f"[dynamic-topk] Cleaned caches for tokens: {','.join(sorted(new_tokens_set))}")
        except Exception as e:
            self.logger().warning(f"[dynamic-topk] Token update cleanup failed: {e}")

    async def _status_dump_loop(self):
        """
        后台线程：每60秒将 format_status() 结果落盘到 logs/status/{script}_status.log
        同时发送 Telegram 通知。
        首次启动会立即落盘一次。
        """
        try:
            dump_dir = Path.cwd() / "logs" / "status"
            dump_dir.mkdir(parents=True, exist_ok=True)
            file_name = f"{getattr(self.config, 'script_file_name', Path(__file__).name).replace('.py','')}_status.log"
            dump_path = dump_dir / file_name
        except Exception as e:
            self.logger().warning(f"[status-dump] Init failed: {e}")
            return

        sent_stopped_arbitrages = set()

        def _write_once():
            try:
                ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
                content = self.format_status()
                with open(dump_path, "a", encoding="utf-8") as fh:
                    fh.write(f"\n===== {ts} =====\n")
                    fh.write(content)
                    fh.write("\n")
                self.logger().debug(f"[status-dump] Wrote status to {dump_path}")

                # send telegram notification
                if self.status_stopped_arbitrage_info:
                    telegram_message = ""
                    new_stopped_arbitrages_found = False
                    for item in self.status_stopped_arbitrage_info:
                        key = item['Stop Time'] + item['Token']
                        if key not in sent_stopped_arbitrages and item['Ex Closed']:
                            sent_stopped_arbitrages.add(key)
                            telegram_message += "**New Stopped Funding Arbitrages**\n"
                            telegram_message += "```\n"
                            telegram_message += f"Token           : {item['Token']}\n"
                            telegram_message += f"Conn 1          : {item['Conn 1']}\n"
                            telegram_message += f"Conn 2          : {item['Conn 2']}\n"
                            telegram_message += f"Px Diff         : {item['Px Diff']}\n"
                            telegram_message += f"Ix Diff         : {item['Ix Diff']}\n"
                            telegram_message += f"Fd Diff         : {item['Fd Diff']}\n"
                            telegram_message += f"Open Delay      : {item['Open Delay']}\n"
                            telegram_message += f"Open Sllipage   : {item['Open Sllipage']}\n"
                            telegram_message += f"Close Delay     : {item['Close Delay']}\n"
                            telegram_message += f"Close Sllipage  : {item['Close Sllipage']}\n"
                            telegram_message += f"Close Type      : {item['Close Type']}\n"
                            telegram_message += f"Fd Pnl          : {item['Fd Pnl']}\n"
                            telegram_message += f"Td Pnl          : {item['Td Pnl']}\n"
                            telegram_message += f"SR              : {item['SR']}\n"
                            telegram_message += f"Hold Time       : {item['Hold Time']}\n"
                            telegram_message += f"Stop Time       : {item['Stop Time']}\n"
                            telegram_message += "```\n"
                            new_stopped_arbitrages_found = True
                            break
                    if new_stopped_arbitrages_found:
                        if self.status_active_arbitrage_info:
                            telegram_message += f"\n**Current Active Arbitrages**\n"
                            telegram_message += "```\n"
                            for active_arbitrage_info in self.status_active_arbitrage_info:
                                telegram_message += f"Hold Time : {active_arbitrage_info['Hold Time']} | "
                                telegram_message += f"Token : {active_arbitrage_info['Token']}\n"
                            telegram_message += "```\n"
                        balances_info = self.get_balances_info()
                        telegram_message += "\n**Current USDT Balances**\n"
                        telegram_message += "```\n"
                        telegram_message += format_df_for_printout(df=pd.DataFrame(balances_info), table_format="psql",)
                        telegram_message += "```\n"
                        telegram_message += f"\n```\n{self.format_utc(self.current_timestamp)}\n```\n"
                        self._send_telegram(telegram_message)
            except Exception as e:
                self.logger().warning(f"[status-dump] Write failed: {e}")

        # 首次立即写一次
        while not self.ready_to_trade:
            self.logger().debug(f"[status-dump] Waiting for ready to trade...")
            await asyncio.sleep(1)
            continue
        _write_once()
        try:
            balances_info = self.get_balances_info()
            telegram_message = "**Starting USDT Balances**\n"
            telegram_message += "```\n"
            telegram_message += format_df_for_printout(df=pd.DataFrame(balances_info), table_format="psql",)
            telegram_message += "```\n"
            telegram_message += f"\n```\n{self.format_utc(self.current_timestamp)}\n```\n"
            self._send_telegram(telegram_message)
        except Exception as e:
            self.logger().warning(f"[status-dump] Send failed: {e}")
            pass
        # 之后每60秒写一次
        while True:
            _write_once()
            try:
                self._maybe_send_daily_pnl_reminder()
            except Exception as e:
                self.logger().debug(f"[daily-pnl] failed: {e}")
            await asyncio.sleep(60)

    async def on_stop(self):
        # 停止状态落盘线程
        try:
            if self._status_dump_thread is not None and not self._status_dump_thread.done():
                self._status_dump_thread.cancel()
                try:
                    await self._status_dump_thread
                except asyncio.CancelledError:
                    pass
                self.logger().info("[status-dump] Status dump thread stopped")
        except Exception as e:
            self.logger().warning(f"[status-dump] Failed to stop thread: {e}")
        # 停止动态TopK扫描任务，避免策略停止后继续重置市场订阅
        try:
            if self._dynamic_scan_task is not None and not self._dynamic_scan_task.done():
                self._dynamic_scan_task.cancel()
                try:
                    await self._dynamic_scan_task
                except asyncio.CancelledError:
                    pass
                self.logger().info("[dynamic-topk] Scanner task stopped")
        except Exception as e:
            self.logger().warning(f"[dynamic-topk] Failed to stop scanner: {e}")
        # 停止内存监控
        try:
            if self._mem_monitor is not None:
                self._mem_monitor.stop()
        except Exception:
            pass

        await super().on_stop()

    async def _compute_topk_via_rest(self):
        """
        Compute Top-K tokens by expected profitability using REST (prices + funding info) across current connectors.
        Only considers tokens tradable on all configured connectors.
        """
        core = TradingCore.get_instance()
        connector_names = list(self.connectors.keys())
        self.logger().info(f"[dynamic-topk] Starting REST scan across connectors: {connector_names}")

        async def _do_scan(temp_connectors: Dict[str, ConnectorBase]):
            # 1) Collect supported pairs per connector
            supported_pairs = {name: set(ex.trading_rules.keys()) for name, ex in temp_connectors.items()}
            self.logger().info("[dynamic-topk] Supported pairs sizes: " + ", ".join(
                f"{k}={len(v)}" for k, v in supported_pairs.items()))

            # 2) Build base->pair mapping per connector
            base_to_pair: Dict[str, Dict[str, str]] = {}
            for name, pairs in supported_pairs.items():
                for pair in pairs:
                    try:
                        base, quote = pair.split("-")
                    except Exception:
                        continue
                    if self.quote_markets_map.get(name, 'USDT') != quote:
                        continue
                    delisting_time = temp_connectors[name].trading_rules[pair].perpetual_delisting_time_seconds
                    if delisting_time is not None and delisting_time > 0:
                        self.logger().debug(f"[dynamic-topk] Skip {pair} in {name} due to delisting")
                        continue
                    d = base_to_pair.setdefault(base, {})
                    d[name] = pair

            # 3) 遍历每个base，然后遍历每两个支持的connector
            results = []
            num_base_scanned = 0
            num_total_base = len(base_to_pair)
            for base, conn_pair_map in base_to_pair.items():
                await asyncio.sleep(1)
                num_base_scanned += 1
                if num_base_scanned % 100 == 0:
                    self.logger().info(f"[dynamic-topk] Scanning base {base} ({num_base_scanned}/{num_total_base})")
                conn_names = list(conn_pair_map.keys())
                for i in range(len(conn_names)):
                    for j in range(len(conn_names)):
                        if i == j:
                            continue
                        c1, c2 = conn_names[i], conn_names[j]
                        p1 = conn_pair_map[c1]
                        p2 = conn_pair_map[c2]
                        try:
                            # Prices
                            price_1 = Decimal(str(await temp_connectors[c1]._get_last_traded_price(p1)))
                            price_2 = Decimal(str(await temp_connectors[c2]._get_last_traded_price(p2)))

                            # Funding
                            f1 = await temp_connectors[c1]._orderbook_ds.get_funding_info(p1)
                            f2 = await temp_connectors[c2]._orderbook_ds.get_funding_info(p2)

                            t1 = f1.next_funding_utc_timestamp
                            t2 = f2.next_funding_utc_timestamp
                            if abs(t1 - t2) > 60:
                                self.logger().debug(f"[dynamic-topk] Skip {base} ({c1}->{c2}) due to time difference: {self.format_utc(t1)} - {self.format_utc(t2)}")
                                continue

                            time_to_funding_1 = t1 - time.time()
                            time_to_funding_2 = t2 - time.time()
                            if time_to_funding_1 / 60 - 60 > self.config.max_time_to_next_funding \
                                or time_to_funding_2 / 60 - 60> self.config.max_time_to_next_funding:
                                self.logger().debug(f"[dynamic-topk] Skip {base} ({c1}->{c2}) due to time to funding: {self.format_utc(time_to_funding_1)} - {self.format_utc(time_to_funding_2)}")
                                continue

                            # Fees (taker, market, open)
                            amt_1 = self.config.position_size_quote / price_1 if price_1 > 0 else Decimal("0")
                            amt_2 = self.config.position_size_quote / price_2 if price_2 > 0 else Decimal("0")
                            fee_1 = temp_connectors[c1].get_fee(
                                base_currency=base, quote_currency=p1.split("-")[1], order_type=OrderType.MARKET,
                                order_side=TradeType.BUY, amount=amt_1, price=price_1, is_maker=False,
                                position_action=PositionAction.OPEN).percent
                            fee_2 = temp_connectors[c2].get_fee(
                                base_currency=base, quote_currency=p2.split("-")[1], order_type=OrderType.MARKET,
                                order_side=TradeType.BUY, amount=amt_2, price=price_2, is_maker=False,
                                position_action=PositionAction.OPEN).percent

                            i_price_1 = f1.index_price
                            i_price_2 = f2.index_price
                            i_price_diff = max(0, i_price_2 - i_price_1)

                            # Direction: BUY on c1, SELL on c2
                            price_profit = (price_2 - price_1 - i_price_diff) / price_1
                            funding_profit = f2.rate - f1.rate
                            trade_profit = price_profit + funding_profit - fee_1 * 2 - fee_2 * 2

                            # if funding_profit >= self.config.min_funding_profitability:
                            results.append({
                                "base": base, "buy": c1, "sell": c2, "p_buy": p1, "p_sell": p2,
                                "profit": trade_profit, "rates": (f1.rate, f2.rate), "prices": (price_1, price_2),
                                "fees": (fee_1, fee_2)
                            })
                        except Exception as e:
                            self.logger().debug(f"[dynamic-topk] Skip {base} ({c1}->{c2}) due to error: {e}")

            # Sort and take Top-K
            results.sort(key=lambda x: float(x["profit"]), reverse=True)
            topk = results[: int(self.config.topk)]
            return topk

        topk = await core.with_temp_connectors(connector_names, _do_scan)
        self._latest_topk_debug = topk
        for entry in topk:
            profit_str = self.format_percent(entry["profit"])
            rates_str = f"({self.format_percent(entry['rates'][0])}, {self.format_percent(entry['rates'][1])})"
            prices_str = f"({entry['prices'][0]:.7f}, {entry['prices'][1]:.7f})"
            fees_str = f"({self.format_percent(entry['fees'][0])}, {self.format_percent(entry['fees'][1])})"
            self.logger().info(
                f"[dynamic-topk] Base: {entry['base']} | "
                f"Buy:{entry['buy']} | "
                f"Sell:{entry['sell']} | "
                f"Profit:{profit_str} | "
                f"Rates:{rates_str} | "
                f"Prices:{prices_str} | "
                f"Fees:{fees_str}"
            )
        bases = [r["base"] for r in topk]
        self._dynamic_topk_tokens = set(bases)
        self.logger().info("[dynamic-topk] TopK bases: " + ",".join(bases))

        # Build target markets list per connector
        target: Dict[str, List[str]] = {name: [] for name in self.config.connectors}
        for r in topk:
            target[r["buy"]].append(r["p_buy"])
            target[r["sell"]].append(r["p_sell"])
        market_names = [(name, list(sorted(set(pairs)))) for name, pairs in target.items()]
        self.logger().info("[dynamic-topk] Reinitialize markets with: " + "; ".join(
            f"{n}={len(ps)}" for n, ps in market_names))

        for name, pairs in market_names:
            self.logger().info(f"[dynamic-topk] {name} pairs: {pairs}")
        
        # Initialize stopped funding arbitrages for new tokens
        for token in self._dynamic_topk_tokens:
            if token not in self.stopped_funding_arbitrages:
                self.stopped_funding_arbitrages[token] = []

        # Apply
        await core.reinitialize_markets(market_names)

        # Are there more things to do here?
        # Cleanup caches and per-token states after WS markets refreshed
        try:
            self._on_tokens_updated_cleanup(self._dynamic_topk_tokens)
        except Exception as e:
            self.logger().warning(f"[dynamic-topk] Cleanup after token update failed: {e}")

    async def _dynamic_scan_loop(self):
        """
        Background loop to periodically scan all symbols via REST, select Top-K by expected profitability,
        and refresh WS subscriptions by rebuilding connectors through TradingCore.
        """
        from datetime import datetime, timedelta
        self.logger().info(f"[dynamic-topk] Scanner loop initialized: every {self.config.scan_interval_hours}h on the hour.")
        is_first_scan = True
        while True:
            try:
                if not self.ready_to_trade:
                    self.logger().debug(f"[dynamic-topk] Waiting for ready to trade...")
                    await asyncio.sleep(1)
                    continue

                if not is_first_scan:
                    now = datetime.utcnow()
                    next_half_hour = (now.replace(minute=30, second=0, microsecond=0) + timedelta(hours=1))
                    sleep_secs = (next_half_hour - now).total_seconds()
                    await asyncio.sleep(sleep_secs)

                    hour = next_half_hour.hour
                    if hour % int(self.config.scan_interval_hours) != 0:
                        self.logger().debug(f"[dynamic-topk] Skipping hour {hour}, not interval boundary.")
                        continue

                # TODO: Uncomment this when we want to stop creating actions
                # self.is_stopping_creating_actions = True
                if len(self.active_funding_arbitrages) > 0:
                    self.logger().debug(f"[dynamic-topk] Skipping REST scan because there are active arbitrages...")
                    await asyncio.sleep(5)
                    continue

                self.is_stopping_creating_actions = True
                self.logger().info("[dynamic-topk] Stopping creating actions...")

                is_first_scan = False

                self.logger().info("[dynamic-topk] Triggering REST scan")
                await self._compute_topk_via_rest()
                self.logger().info("[dynamic-topk] REST scan completed")
                self.is_stopping_creating_actions = False # reset

            except asyncio.CancelledError:
                self.logger().info("[dynamic-topk] Scanner task cancelled.")
                break
            except Exception as e:
                self.logger().error(f"[dynamic-topk] Scanner loop error: {e}")
