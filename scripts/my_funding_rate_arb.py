import os
from decimal import Decimal
from typing import Dict, List, Set
from datetime import datetime, timedelta
import time

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

COOL_DOWN_COUNT = 60 * 60 * 24

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

    def start(self, clock: Clock, timestamp: float) -> None:
        """
        Start the strategy.
        :param clock: Clock to use.
        :param timestamp: Current time.
        """
        self._last_timestamp = timestamp
        self.apply_initial_setting()
        # Kick off dynamic scanner if enabled
        if getattr(self.config, "dynamic_topk_enabled", False):
            try:
                import asyncio
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

    async def _dynamic_scan_loop(self):
        """
        Background loop to periodically scan all symbols via REST, select Top-K by expected profitability,
        and refresh WS subscriptions by rebuilding connectors through TradingCore.
        This initial implementation only logs scheduling and placeholders; REST scan logic is added in a later commit.
        """
        import asyncio
        from datetime import datetime, timedelta
        self.logger().info(f"[dynamic-topk] Scanner loop initialized: every {self.config.scan_interval_hours}h on the hour.")
        # Align to next full hour
        while True:
            try:
                now = datetime.utcnow()
                next_hour = (now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
                sleep_secs = (next_hour - now).total_seconds()
                await asyncio.sleep(sleep_secs)

                # Check if this hour matches the interval boundary
                hour = next_hour.hour
                if hour % int(self.config.scan_interval_hours) != 0:
                    self.logger().debug(f"[dynamic-topk] Skipping hour {hour}, not interval boundary.")
                    continue

                self.logger().info("[dynamic-topk] Triggering REST scan (placeholder)")
                # Placeholder: REST scan + Top-K selection will be implemented in next commit
            except asyncio.CancelledError:
                self.logger().info("[dynamic-topk] Scanner task cancelled.")
                break
            except Exception as e:
                self.logger().error(f"[dynamic-topk] Scanner loop error: {e}")

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
        for token in self.config.tokens:
            if token not in self.active_funding_arbitrages:
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
            self.logger().debug(f"{token} executors_pnl={executors_pnl:.4%}, by_hand={executors_pnl_by_hand:.4%}")
            executors_trade_pnl = sum(executor.custom_info['trade_pnl_pct'] for executor in executors)
            trade_pnl_by_had = (a_price_2 - a_price_1 - c_price_2 + c_price_1) / price_1
            self.logger().debug(f"{executors_trade_pnl=:.4%}, by_hand={trade_pnl_by_had:.4%}")
            self.logger().debug(f"{a_price_1=:.7f},{a_price_2=:.7f},{c_price_1=:.7f},{c_price_2=:.7f}")
            executor_1, executor_2 = executors
            self.logger().debug(f"{executor_1.custom_info['entry_price']=:.7f},{executor_2.custom_info['entry_price']=:.7f}")
            self.logger().debug(f"{executor_1.custom_info['close_price']=:.7f},{executor_2.custom_info['close_price']=:.7f}")
            funding_info_report = self.get_funding_info_by_token(token)
            rate_1 = funding_info_report[connector_1].rate
            rate_2 = funding_info_report[connector_2].rate
            take_profit_condition = executors_pnl_by_hand + funding_payments_pnl_pct > \
                                    self.config.min_take_profit + fee_1 + fee_2
            keep_holding_condition = rate_2 - rate_1 > trade_pnl_by_had and rate_2 - rate_1 > 0
            keep_holding_condition = keep_holding_condition or (self.config.min_funding_profitability > 0 and len(funding_arbitrage_info["funding_payments"]) < 2)
            if take_profit_condition and keep_holding_condition:
                self.logger().info("TP reached but holding")
            take_profit_condition = take_profit_condition and not keep_holding_condition
            # TODO strengthen stop_loss_condition
            stop_loss_condition = len(funding_arbitrage_info["funding_payments"]) > 1 \
                                and rate_2 - rate_1 < self.config.min_funding_profitability
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
            for token in self.config.tokens:
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
