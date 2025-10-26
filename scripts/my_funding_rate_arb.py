import os
import asyncio
from decimal import Decimal
from typing import Dict, List, Set
from datetime import datetime, timedelta
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

COOL_DOWN_COUNT = 60 * 60 * 24

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

    def start(self):
        if not _HAS_TRACEMALLOC:
            self._logger().warning("[mem] tracemalloc unavailable; memory snapshots disabled")
            return
        try:
            if not self._tracemalloc_started:
                # Keep up to 25 frames for better grouping; adjust if overhead is a concern
                tracemalloc.start(5)
                self._tracemalloc_started = True
        except Exception as e:
            self._logger().warning(f"[mem] Failed to start tracemalloc: {e}")
            return

        # Try to install a SIGUSR1 handler for manual dumps (best-effort)
        if _HAS_SIGNAL:
            try:
                signal.signal(signal.SIGUSR1, self._handle_sigusr1)
            except Exception:
                # Not in main thread or not supported
                pass

        self._thread = threading.Thread(target=self._run_loop, name="MemoryMonitor", daemon=True)
        self._thread.start()
        self._logger().info("[mem] Memory monitor started")

    def stop(self):
        try:
            self._stop.set()
        except Exception:
            pass

    def _handle_sigusr1(self, signum, frame):
        try:
            self._dump_snapshot(reason="signal")
        except Exception as e:
            self._logger().warning(f"[mem] SIGUSR1 dump failed: {e}")

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
                # Periodic dump
                self._dump_snapshot(reason="interval")

                # Threshold check
                if self._rss_threshold_bytes:
                    cur_rss = self._current_rss()
                    if self._last_rss and cur_rss - self._last_rss >= self._rss_threshold_bytes:
                        self._dump_snapshot(reason=f"rss+{(cur_rss - self._last_rss) / (1024*1024):.1f}MB")
                        self._last_rss = cur_rss
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

                # tracemalloc stats
                if _HAS_TRACEMALLOC:
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
                        summary.print_(sum1, stream=fh)
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
                    for o in gc.get_objects():
                        try:
                            t = type(o)
                            if t in (list, dict, set, tuple):
                                size = sys.getsizeof(o)
                                ln = len(o) if hasattr(o, '__len__') else 0
                                mod = getattr(t, '__module__', '')
                                # Attempt to retrieve o's variable name from globals or locals
                                var_name = None
                                try:
                                    for scope in (globals(), locals()):
                                        for k, v in scope.items():
                                            if v is o:
                                                var_name = k
                                                break
                                        if var_name:
                                            break
                                except Exception:
                                    var_name = None
                                sized.append((size, ln, t.__name__, mod, o, var_name))
                        except Exception:
                            continue
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
        "hyperliquid_perpetual": 60 * 60 * 1
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
        self._dynamic_scan_task = None
        self._dynamic_topk_tokens = set(self.config.tokens)
        self._latest_topk_debug = []
        self.is_stopping_creating_actions = False
        self._mem_monitor = None

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
            funding_rates[connector_name] = connector.get_funding_info(trading_pair)
        return funding_rates

    def get_price_and_fee_with_cache(self, prices_and_fees_cache: Dict, connector_name, token: str, side: TradeType):
        if connector_name in prices_and_fees_cache:
            return prices_and_fees_cache[connector_name]
        trading_pair = self.get_trading_pair_for_connector(token, connector_name)
        price = Decimal(self.market_data_provider.get_price_for_quote_volume(
            connector_name=connector_name,
            trading_pair=trading_pair,
            quote_volume=self.config.position_size_quote,
            is_buy=side == TradeType.BUY,
        ).result_price)
        fee = self.connectors[connector_name].get_fee(
            base_currency=trading_pair.split("-")[0],
            quote_currency=trading_pair.split("-")[1],
            order_type=OrderType.MARKET,
            order_side=TradeType.BUY,
            amount=self.config.position_size_quote / price,
            price=price,
            is_maker=False,
            position_action=PositionAction.OPEN
        ).percent
        prices_and_fees_cache[connector_name] = (price, fee)
        return (price, fee)

    def get_most_trade_profitable_combination(self, prices_and_fees_cache: Dict, funding_info_report: Dict, token: str):
        # Remove connectors that are far away from funding time
        valid_connectors = []
        for connector in funding_info_report:
            time_to_funding = funding_info_report[connector].next_funding_utc_timestamp - self.current_timestamp
            if time_to_funding / 60 < self.config.max_time_to_next_funding:
                valid_connectors.append(connector)

        # TODO: computation delay mesure

        # Find best combination
        best_combination = None
        highest_profitability = Decimal(-100)
        for connector_1 in valid_connectors:
            for connector_2 in valid_connectors:
                if connector_1 != connector_2:
                    time_to_funding_1 = funding_info_report[connector_1].next_funding_utc_timestamp - self.current_timestamp
                    time_to_funding_2 = funding_info_report[connector_2].next_funding_utc_timestamp - self.current_timestamp
                    if abs(time_to_funding_1 - time_to_funding_2) > 60:
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
                    price_profit = (price_2 - price_1) / price_1
                    funding_rate_profit = rate_2 - rate_1
                    trade_profit = price_profit + funding_rate_profit - fee_1 * 2 - fee_2 * 2
                    if float(trade_profit) > float(highest_profitability):
                        trade_side = TradeType.BUY
                        highest_profitability = trade_profit
                        best_combination = (connector_1, connector_2, trade_side, trade_profit, \
                                            rate_1, rate_2, price_1, price_2, fee_1, fee_2)
        return best_combination
    
    def enough_balance(self, connector_name):
        connector = self.connectors[connector_name]
        avail_usd = float(connector.available_balances.get(self.quote_markets_map.get(connector_name, 'USDT'), 0))
        return avail_usd >= float(self.config.position_size_quote) / float(self.config.leverage), avail_usd

    def create_actions_proposal(self) -> List[CreateExecutorAction]:
        """
        In this method we are going to evaluate if a new set of positions has to be created for each of the tokens that
        don't have an active arbitrage.
        More filters can be applied to limit the creation of the positions, since the current logic is only checking for
        positive pnl between funding rate. Is logged and computed the trading profitability at the time for entering
        at market to open the possibilities for other people to create variations like sending limit position executors
        and if one gets filled buy market the other one to improve the entry prices.
        """
        create_actions = []
        for token in self.tokens_for_trading():
            if token not in self.active_funding_arbitrages:
                self.token_failure_cool_down.setdefault(token, 0)
                self.token_failure_cool_down[token] -= 1
                if self.token_failure_cool_down[token] > 0:
                    continue
                prices_and_fees_cache = dict()
                funding_info_report = self.get_funding_info_by_token(token)
                best_combination = self.get_most_trade_profitable_combination(prices_and_fees_cache,
                                                                              funding_info_report, token)
                if not best_combination:
                    continue
                connector_1, connector_2, trade_side, expected_profitability, \
                        rate_1, rate_2, price_1, price_2, fee_1, fee_2 = best_combination
                if expected_profitability >= self.config.min_trade_profitability \
                    and rate_2 - rate_1 >= self.config.min_funding_profitability:
                    enough_1, balance_1 = self.enough_balance(connector_1)
                    enough_2, balance_2 = self.enough_balance(connector_2)
                    if not enough_1 or not enough_2:
                        self.logger().warning(f"Balance Not enough for {connector_1 if not enough_1 else connector_2} "
                                              f"({(balance_1 if not enough_1 else balance_2):.3f})"
                                              f", didn't open positions")
                        continue
                    self.logger().info(f"Best Combination: {token} | {connector_1} | {connector_2} | {trade_side} | "
                                       f"rate_1={self.format_percent(rate_1)} | rate_2={self.format_percent(rate_2)} | "
                                       f"price_1={price_1:.7f} | price_2={price_2:.7f} | "
                                       f"fee_1={self.format_percent(fee_1)} | fee_2={self.format_percent(fee_2)} | "
                                       f"balance_1={balance_1:.3f} | balance_2={balance_2:.3f} | "
                                       f"expected_profitability={self.format_percent(expected_profitability)} ")
                    if self.is_stopping_creating_actions:
                        self.logger().debug(f"Stopping creating actions, skipping creation of executors for {token}")
                        continue
                    self.logger().info(f"Starting executors...")
                    position_executor_config_1, position_executor_config_2 = \
                        self.get_position_executors_config(token, connector_1, connector_2, trade_side, price_1, price_2)
                    self.active_funding_arbitrages[token] = {
                        "connector_1": connector_1,
                        "connector_2": connector_2,
                        "rate_1": rate_1,
                        "rate_2": rate_2,
                        "price_1": price_1,
                        "price_2": price_2,
                        "fee_1": fee_1,
                        "fee_2": fee_2,
                        "expected_profitability": expected_profitability,
                        "executors_ids": [position_executor_config_1.id, position_executor_config_2.id],
                        "side": trade_side,
                        "funding_payments": [],
                        "start_time": self.current_timestamp
                    }
                    return [CreateExecutorAction(executor_config=position_executor_config_1),
                            CreateExecutorAction(executor_config=position_executor_config_2)]
        return create_actions

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

    def stop_actions_proposal(self) -> List[StopExecutorAction]:
        """
        Once the funding rate arbitrage is created we are going to control the funding payments pnl and the current
        pnl of each of the executors at the cost of closing the open position at market.
        If that PNL is greater than the profitability_to_take_profit
        """
        stop_executor_actions = []
        stopped_tokens = []
        for token, funding_arbitrage_info in self.active_funding_arbitrages.items():
            executors = self.get_executors(funding_arbitrage_info["executors_ids"])
            closed_ex = list(ex.close_type for ex in executors if ex.close_type)
            if len(closed_ex) > 0:
                self.logger().warning(f"Closed executor for {token} found due to "
                                      f"{','.join([close.name for close in closed_ex])}, stopping executors")
                self.token_failure_cool_down[token] = COOL_DOWN_COUNT
                stopped_tokens.append(token)
                funding_arbitrage_info['stop_reason'] = "UNK"
                self.stopped_funding_arbitrages[token].append(funding_arbitrage_info)
                stop_executor_actions.extend(self.create_stop_executor_action(executors))
                continue
            if len(executors) != 2:
                self.logger().debug(f"Executors not found for {token} ({len(executors)}) when stop actions proposal")
                continue
            funding_payments_pnl = sum(funding_payment.amount for funding_payment in funding_arbitrage_info["funding_payments"])
            funding_payments_pnl_pct = funding_payments_pnl / self.config.position_size_quote
            fee_1, fee_2 = funding_arbitrage_info["fee_1"], funding_arbitrage_info["fee_2"]
            a_price_1 = executors[0].custom_info['actual_open_price']
            a_price_2 = executors[1].custom_info['actual_open_price']
            if not a_price_1 or not a_price_2:
                self.logger().debug(f"Skip stop actions judgement {token} because open-order didn't filled")
                continue
            connector_1 = funding_arbitrage_info["connector_1"]
            connector_2 = funding_arbitrage_info["connector_2"]
            c_price_1, _ = self.get_price_and_fee_with_cache( \
                {}, connector_1, token, TradeType.SELL)
            c_price_2, _ = self.get_price_and_fee_with_cache( \
                {}, connector_2, token, TradeType.BUY)
            executors_pnl = sum(executor.net_pnl_pct for executor in executors)
            price_1 = funding_arbitrage_info['price_1']
            executors_pnl_by_hand = (a_price_2 - a_price_1 - c_price_2 + c_price_1) / price_1 - fee_1 - fee_2
            # self.logger().debug(f"{token} executors_pnl={executors_pnl:.4%}, by_hand={executors_pnl_by_hand:.4%}")
            executors_trade_pnl = sum(executor.custom_info['trade_pnl_pct'] for executor in executors)
            trade_pnl_by_had = (a_price_2 - a_price_1 - c_price_2 + c_price_1) / price_1
            # self.logger().debug(f"{executors_trade_pnl=:.4%}, by_hand={trade_pnl_by_had:.4%}")
            # self.logger().debug(f"{a_price_1=:.7f},{a_price_2=:.7f},{c_price_1=:.7f},{c_price_2=:.7f}")
            executor_1, executor_2 = executors
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

            # TODO strengthen stop_loss_condition
            stop_loss_condition = len(funding_arbitrage_info["funding_payments"]) > 1 \
                                and c_price_2 - c_price_1 < 0
            if take_profit_condition:
                self.logger().info(f"Take profit profitability reached for {token}, stopping executors, "
                                   f"{executors_pnl_by_hand=:.4%}, "
                                   f"{funding_payments_pnl_pct=:.4%}")
                stopped_tokens.append(token)
                funding_arbitrage_info['stop_reason'] = "TP"
                self.stopped_funding_arbitrages[token].append(funding_arbitrage_info)
                stop_executor_actions.extend(self.create_stop_executor_action(executors, c_price_1, c_price_2))
            elif stop_loss_condition:
                self.logger().info(f"Stop loss condition satisfied for {token}, stopping executors")
                stopped_tokens.append(token)
                funding_arbitrage_info['stop_reason'] = "SL"
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
    
    def format_time(self, x) -> str:
        sign = ' ' if x > 0 else '-'
        x = abs(x)
        hours, remainder = divmod(x, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        return f"{sign}{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"


    def format_status(self) -> str:
        original_status = super().format_status()
        funding_rate_status = []
        if self.ready_to_trade:
            all_funding_info = []
            all_best_paths = []
            for token in self.tokens_for_trading():
                
                token_info = {"token": token}
                funding_info_report = self.get_funding_info_by_token(token)
                for connector_name, info in funding_info_report.items():
                    token_info[f"{connector_name} Rate (%)"] = info.rate * 100
                all_funding_info.append(token_info)

                best_paths_info = {"token": token}
                prices_and_fees_cache = dict()
                best_combination = self.get_most_trade_profitable_combination(prices_and_fees_cache, \
                                                                               funding_info_report, token)
                if best_combination:
                    connector_1, connector_2, trade_side, expected_profitability, \
                        rate_1, rate_2, price_1, price_2, fee_1, fee_2 = best_combination
                    best_paths_info["Best Path"] = f"{connector_1}_{connector_2}"
                    best_paths_info["Pirce Diff"] = self.format_percent((price_2 - price_1) / price_1)
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

            funding_rate_status.append(f"\nActive Funding Arbitrages:")
            active_arbitrage_info = []
            active_arbitrage_debug = []
            for token, funding_arbitrage_info in self.active_funding_arbitrages.items():
                arbitrage_info = { "token": token }
                arbitrage_info["Connector 1"] = funding_arbitrage_info["connector_1"]
                arbitrage_info["Connector 2"] = funding_arbitrage_info["connector_2"]
                funding_payments_pnl = \
                    sum(funding_payment.amount for funding_payment in funding_arbitrage_info["funding_payments"]) \
                    / self.config.position_size_quote
                price_1, price_2 = funding_arbitrage_info["price_1"], funding_arbitrage_info["price_2"]
                rate_1, rate_2 = funding_arbitrage_info["rate_1"], funding_arbitrage_info["rate_2"]
                fee_1, fee_2 = funding_arbitrage_info["fee_1"], funding_arbitrage_info["fee_2"]
                arbitrage_info["Px Diff"] = self.format_percent((price_2 - price_1) / price_1)
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
            funding_rate_status.append( \
                format_df_for_printout(df=pd.DataFrame(active_arbitrage_info), table_format="psql",))
            funding_rate_status.append( \
                format_df_for_printout(df=pd.DataFrame(active_arbitrage_debug), table_format="psql",))

            funding_rate_status.append(f"\nStopped Funding Arbitrages:")
            stopped_arbitrage_info = []
            for token, funding_arbitrage_infos in self.stopped_funding_arbitrages.items():
                for funding_arbitrage_info in funding_arbitrage_infos:
                    arbitrage_info = {'token': token}
                    connector_1 = funding_arbitrage_info["connector_1"]
                    connector_2 = funding_arbitrage_info["connector_2"]
                    arbitrage_info['Connector 1'] = connector_1
                    arbitrage_info['Connector 2'] = connector_2
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
                    arbitrage_info['Fund Pnl'] = self.format_percent(funding_payments_pnl)
                    arbitrage_info['Trade Pnl'] = self.format_percent(executors_pnl)
                    arbitrage_info['TS'] = funding_arbitrage_info['stop_reason']

                    stopped_arbitrage_info.append(arbitrage_info)
            funding_rate_status.append( \
                format_df_for_printout(df=pd.DataFrame(stopped_arbitrage_info), table_format="psql",))
        return original_status + "\n".join(funding_rate_status)

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

                            # Direction: BUY on c1, SELL on c2
                            price_profit = (price_2 - price_1) / price_1
                            funding_profit = f2.rate - f1.rate
                            trade_profit = price_profit + funding_profit - fee_1 * 2 - fee_2 * 2

                            if funding_profit >= self.config.min_funding_profitability:
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

                self.is_stopping_creating_actions = True
                if len(self.active_funding_arbitrages) > 0:
                    self.logger().debug(f"[dynamic-topk] Skipping REST scan because there are active arbitrages...")
                    await asyncio.sleep(5)
                    continue

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
