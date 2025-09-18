import os
from decimal import Decimal
from typing import Dict, List, Set

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
        "okx_perpetual" : PositionMode.ONEWAY,
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

    def start(self, clock: Clock, timestamp: float) -> None:
        """
        Start the strategy.
        :param clock: Clock to use.
        :param timestamp: Current time.
        """
        self._last_timestamp = timestamp
        self.apply_initial_setting()

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
        # Find best combination
        best_combination = None
        highest_profitability = -100
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
                    if trade_profit > highest_profitability:
                        trade_side = TradeType.BUY
                        highest_profitability = trade_profit
                        best_combination = (connector_1, connector_2, trade_side, trade_profit, \
                                            rate_1, rate_2, price_1, price_2, fee_1, fee_2)
        return best_combination

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
                prices_and_fees_cache = dict()
                funding_info_report = self.get_funding_info_by_token(token)
                best_combination = self.get_most_trade_profitable_combination(prices_and_fees_cache,
                                                                              funding_info_report, token)
                if not best_combination:
                    continue
                connector_1, connector_2, trade_side, expected_profitability, \
                        rate_1, rate_2, price_1, price_2, fee_1, fee_2 = best_combination
                if expected_profitability >= self.config.min_trade_profitability:
                    self.logger().info(f"Best Combination: {connector_1} | {connector_2} | {trade_side} | "
                                       f"rate_1={rate_1} | rate_2={rate_2} | price_1={price_1} | price_2={price_2} |"
                                       f"fee_1={fee_1} | fee_2={fee_2} | expected_profitability={expected_profitability} "
                                       f"Starting executors...")
                    position_executor_config_1, position_executor_config_2 = \
                            self.get_position_executors_config(token, connector_1, connector_2, trade_side)
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
                    }
                    return [CreateExecutorAction(executor_config=position_executor_config_1),
                            CreateExecutorAction(executor_config=position_executor_config_2)]
        return create_actions

    def stop_actions_proposal(self) -> List[StopExecutorAction]:
        """
        Once the funding rate arbitrage is created we are going to control the funding payments pnl and the current
        pnl of each of the executors at the cost of closing the open position at market.
        If that PNL is greater than the profitability_to_take_profit
        """
        stop_executor_actions = []
        stopped_tokens = []
        for token, funding_arbitrage_info in self.active_funding_arbitrages.items():
            executors = self.filter_executors(
                executors=self.get_all_executors(),
                filter_func=lambda x: x.id in funding_arbitrage_info["executors_ids"]
            )
            funding_payments_pnl = sum(funding_payment.amount for funding_payment in funding_arbitrage_info["funding_payments"])
            executors_pnl = sum(executor.net_pnl_quote for executor in executors)
            fee_1, fee_2 = funding_arbitrage_info["fee_1"], funding_arbitrage_info["fee_2"]
            take_profit_pnl_threshold = \
                (self.config.min_trade_profitability + fee_1 + fee_2) * self.config.position_size_quote
            take_profit_condition = executors_pnl + funding_payments_pnl > take_profit_pnl_threshold
            # TODO strengthen stop_loss_condition
            stop_loss_condition = len(funding_arbitrage_info["funding_payments"]) > 1
            if take_profit_condition:
                self.logger().info("Take profit profitability reached, stopping executors, "
                                   f"{executors_pnl=:.4f}, {funding_payments_pnl=:.4f}, {take_profit_pnl_threshold=:.4f}")
                stopped_tokens.append(token)
                self.stopped_funding_arbitrages[token].append(funding_arbitrage_info)
                stop_executor_actions.extend([StopExecutorAction(executor_id=executor.id) for executor in executors])
            elif stop_loss_condition:
                self.logger().info("Stop loss condition satisfied, stopping executors")
                stopped_tokens.append(token)
                self.stopped_funding_arbitrages[token].append(funding_arbitrage_info)
                stop_executor_actions.extend([StopExecutorAction(executor_id=executor.id) for executor in executors])
        for token in stopped_tokens:
            self.active_funding_arbitrages.pop(token, None)
        return stop_executor_actions

    def did_complete_funding_payment(self, funding_payment_completed_event: FundingPaymentCompletedEvent):
        """
        Based on the funding payment event received, check if one of the active arbitrages matches to add the event
        to the list.
        """
        token = funding_payment_completed_event.trading_pair.split("-")[0]
        if token in self.active_funding_arbitrages:
            self.active_funding_arbitrages[token]["funding_payments"].append(funding_payment_completed_event)

    def get_position_executors_config(self, token, connector_1, connector_2, trade_side):
        price = self.market_data_provider.get_price_by_type(
            connector_name=connector_1,
            trading_pair=self.get_trading_pair_for_connector(token, connector_1),
            price_type=PriceType.MidPrice
        )
        position_amount = self.config.position_size_quote / price

        position_executor_config_1 = PositionExecutorConfig(
            timestamp=self.current_timestamp,
            connector_name=connector_1,
            trading_pair=self.get_trading_pair_for_connector(token, connector_1),
            side=trade_side,
            amount=position_amount,
            leverage=self.config.leverage,
            triple_barrier_config=TripleBarrierConfig(open_order_type=OrderType.MARKET),
        )
        position_executor_config_2 = PositionExecutorConfig(
            timestamp=self.current_timestamp,
            connector_name=connector_2,
            trading_pair=self.get_trading_pair_for_connector(token, connector_2),
            side=TradeType.BUY if trade_side == TradeType.SELL else TradeType.SELL,
            amount=position_amount,
            leverage=self.config.leverage,
            triple_barrier_config=TripleBarrierConfig(open_order_type=OrderType.MARKET),
        )
        return position_executor_config_1, position_executor_config_2

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
                    best_paths_info["Pirce Diff"] = f"{(price_2 - price_1) / price_1:.3%}"
                    best_paths_info["Rate Diff"] = f"{(rate_2 - rate_1):.3%}"
                    best_paths_info["Fees"] = f"{(fee_1 + fee_2):.3%}"
                    best_paths_info["Trade Profit"] = f"{expected_profitability:.3%}"

                    time_to_next_funding_info_c1 = funding_info_report[connector_1].next_funding_utc_timestamp - self.current_timestamp
                    time_to_next_funding_info_c2 = funding_info_report[connector_2].next_funding_utc_timestamp - self.current_timestamp
                    best_paths_info["Time to Funding 1"] = f"{time_to_next_funding_info_c1 / 60:.1f}"
                    best_paths_info["Time to Funding 2"] = f"{time_to_next_funding_info_c2 / 60:.1f}"
                    all_best_paths.append(best_paths_info)

            funding_rate_status.append(f"\n\n\nMin Trade Profitability: {self.config.min_trade_profitability:.2%}")
            funding_rate_status.append("Funding Rate Info")
            funding_rate_status.append(format_df_for_printout(df=pd.DataFrame(all_funding_info), table_format="psql",))
            funding_rate_status.append(format_df_for_printout(df=pd.DataFrame(all_best_paths), table_format="psql",))
            for token, funding_arbitrage_info in self.active_funding_arbitrages.items():
                long_connector = funding_arbitrage_info["connector_1"] if funding_arbitrage_info["side"] == TradeType.BUY else funding_arbitrage_info["connector_2"]
                short_connector = funding_arbitrage_info["connector_2"] if funding_arbitrage_info["side"] == TradeType.BUY else funding_arbitrage_info["connector_1"]
                funding_rate_status.append(f"Token: {token}")
                funding_rate_status.append(f"Long connector: {long_connector} | Short connector: {short_connector}")
                funding_rate_status.append(f"Funding Payments Collected: {funding_arbitrage_info['funding_payments']}")
                funding_rate_status.append(f"Executors: {funding_arbitrage_info['executors_ids']}")
                funding_rate_status.append("-" * 50 + "\n")
        return original_status + "\n".join(funding_rate_status)
