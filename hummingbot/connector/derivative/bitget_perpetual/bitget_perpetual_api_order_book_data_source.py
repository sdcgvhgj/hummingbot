import asyncio
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from hummingbot.connector.derivative.bitget_perpetual import (
    bitget_perpetual_constants as CONSTANTS,
    bitget_perpetual_web_utils as web_utils,
)
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.funding_info import FundingInfo, FundingInfoUpdate
from hummingbot.core.data_type.order_book import OrderBookMessage
from hummingbot.core.data_type.order_book_message import OrderBookMessageType
from hummingbot.core.data_type.perpetual_api_order_book_data_source import PerpetualAPIOrderBookDataSource
from hummingbot.core.utils.tracking_nonce import NonceCreator
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from hummingbot.connector.derivative.bitget_perpetual.bitget_perpetual_derivative import BitgetPerpetualDerivative


class BitgetPerpetualAPIOrderBookDataSource(PerpetualAPIOrderBookDataSource):

    def __init__(
        self,
        trading_pairs: List[str],
        connector: 'BitgetPerpetualDerivative',
        api_factory: WebAssistantsFactory,
    ):
        super().__init__(trading_pairs)
        self._connector = connector
        self._api_factory = api_factory
        self._nonce_provider = NonceCreator.for_microseconds()

    async def get_last_traded_prices(self, trading_pairs: List[str], domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def get_funding_info(self, trading_pair: str) -> FundingInfo:
        symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        product_type = await self._connector.product_type_associated_to_trading_pair(trading_pair)

        rest_assistant = await self._api_factory.get_rest_assistant()

        # Get ticker info for mark price, index price, last price
        ticker_url = web_utils.get_rest_url_for_endpoint(endpoint=CONSTANTS.PUBLIC_TICKER_ENDPOINT)
        ticker_response = await rest_assistant.execute_request(
            url=ticker_url,
            throttler_limit_id=CONSTANTS.PUBLIC_TICKER_ENDPOINT,
            params={"symbol": symbol, "productType": product_type},
            method=RESTMethod.GET,
        )
        ticker_data = ticker_response["data"][0]

        # Get funding rate
        funding_url = web_utils.get_rest_url_for_endpoint(endpoint=CONSTANTS.PUBLIC_FUNDING_RATE_ENDPOINT)
        funding_response = await rest_assistant.execute_request(
            url=funding_url,
            throttler_limit_id=CONSTANTS.PUBLIC_FUNDING_RATE_ENDPOINT,
            params={"symbol": symbol, "productType": product_type},
            method=RESTMethod.GET,
        )
        funding_data = funding_response["data"][0]

        # Get funding time for next funding timestamp
        funding_time_url = web_utils.get_rest_url_for_endpoint(endpoint=CONSTANTS.PUBLIC_FUNDING_TIME_ENDPOINT)
        funding_time_response = await rest_assistant.execute_request(
            url=funding_time_url,
            throttler_limit_id=CONSTANTS.PUBLIC_FUNDING_TIME_ENDPOINT,
            params={"symbol": symbol, "productType": product_type},
            method=RESTMethod.GET,
        )
        funding_time_data = funding_time_response["data"][0]

        # Determine funding interval from ratePeriod if available
        funding_interval = None
        rate_period = funding_time_data.get("ratePeriod")
        if rate_period is not None:
            try:
                funding_interval = int(rate_period) * 60
            except (ValueError, TypeError):
                pass

        funding_info = FundingInfo(
            trading_pair=trading_pair,
            index_price=Decimal(str(ticker_data.get("indexPrice", "0"))),
            mark_price=Decimal(str(ticker_data.get("markPrice", ticker_data.get("lastPr", "0")))),
            next_funding_utc_timestamp=int(funding_time_data.get("nextFundingTime", "0")) // 1000,
            rate=Decimal(str(funding_data.get("fundingRate", "0"))),
            funding_interval=funding_interval,
        )
        return funding_info

    async def listen_for_subscriptions(self):
        ws: Optional[WSAssistant] = None
        while True:
            try:
                ws = await self._get_connected_websocket_assistant(web_utils.get_ws_public_url())
                await self._subscribe_to_channels(ws, self._trading_pairs)
                await self._process_websocket_messages(ws)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception(
                    "Unexpected error occurred when listening to order book streams. Retrying in 5 seconds..."
                )
                await self._sleep(5.0)
            finally:
                ws and await ws.disconnect()

    async def _get_connected_websocket_assistant(self, ws_url: str) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(
            ws_url=ws_url, message_timeout=CONSTANTS.SECONDS_TO_WAIT_TO_RECEIVE_MESSAGE
        )
        return ws

    async def _subscribe_to_channels(self, ws: WSAssistant, trading_pairs: List[str]):
        try:
            for trading_pair in trading_pairs:
                symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                product_type = await self._connector.product_type_associated_to_trading_pair(trading_pair)
                inst_type = self._product_type_to_inst_type(product_type)

                subscribe_books = {
                    "op": "subscribe",
                    "args": [{"instType": inst_type, "channel": CONSTANTS.PUBLIC_WS_BOOKS, "instId": symbol}],
                }
                subscribe_trades = {
                    "op": "subscribe",
                    "args": [{"instType": inst_type, "channel": CONSTANTS.PUBLIC_WS_TRADE, "instId": symbol}],
                }
                subscribe_ticker = {
                    "op": "subscribe",
                    "args": [{"instType": inst_type, "channel": CONSTANTS.PUBLIC_WS_TICKER, "instId": symbol}],
                }

                await ws.send(WSJSONRequest(payload=subscribe_books))
                await ws.send(WSJSONRequest(payload=subscribe_trades))
                await ws.send(WSJSONRequest(payload=subscribe_ticker))

            self.logger().info("Subscribed to public order book, trade and ticker channels...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error occurred subscribing to order book streams...")
            raise

    @staticmethod
    def _product_type_to_inst_type(product_type: str) -> str:
        mapping = {
            CONSTANTS.USDT_PRODUCT_TYPE: "USDT-FUTURES",
            CONSTANTS.USDC_PRODUCT_TYPE: "USDC-FUTURES",
            CONSTANTS.USD_PRODUCT_TYPE: "COIN-FUTURES",
        }
        return mapping.get(product_type, "USDT-FUTURES")

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant):
        while True:
            try:
                await super()._process_websocket_messages(websocket_assistant=websocket_assistant)
            except asyncio.TimeoutError:
                ping_request = WSJSONRequest(payload=CONSTANTS.PUBLIC_WS_PING_REQUEST)
                await websocket_assistant.send(ping_request)

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        channel = ""
        if isinstance(event_message, str):
            return channel
        if "arg" in event_message and "data" in event_message:
            event_channel = event_message["arg"].get("channel", "")
            action = event_message.get("action", "")
            if event_channel == CONSTANTS.PUBLIC_WS_TRADE:
                channel = self._trade_messages_queue_key
            elif event_channel == CONSTANTS.PUBLIC_WS_BOOKS:
                if action == "snapshot":
                    channel = self._snapshot_messages_queue_key
                else:
                    channel = self._diff_messages_queue_key
            elif event_channel == CONSTANTS.PUBLIC_WS_TICKER:
                channel = self._funding_info_messages_queue_key
        return channel

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        action = raw_message.get("action", "")
        if action == "update":
            symbol = raw_message["arg"]["instId"]
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol)
            data = raw_message["data"][0]
            timestamp_seconds = int(data.get("ts", 0)) / 1e3
            update_id = self._nonce_provider.get_tracking_nonce(timestamp=timestamp_seconds)

            bids, asks = self._get_bids_and_asks(data)
            order_book_message_content = {
                "trading_pair": trading_pair,
                "update_id": update_id,
                "bids": bids,
                "asks": asks,
            }
            diff_message = OrderBookMessage(
                message_type=OrderBookMessageType.DIFF,
                content=order_book_message_content,
                timestamp=timestamp_seconds,
            )
            message_queue.put_nowait(diff_message)

    async def _parse_order_book_snapshot_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        action = raw_message.get("action", "")
        if action == "snapshot":
            symbol = raw_message["arg"]["instId"]
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol)
            data = raw_message["data"][0]
            timestamp_seconds = int(data.get("ts", 0)) / 1e3
            update_id = self._nonce_provider.get_tracking_nonce(timestamp=timestamp_seconds)

            bids, asks = self._get_bids_and_asks(data)
            order_book_message_content = {
                "trading_pair": trading_pair,
                "update_id": update_id,
                "bids": bids,
                "asks": asks,
            }
            snapshot_msg = OrderBookMessage(
                message_type=OrderBookMessageType.SNAPSHOT,
                content=order_book_message_content,
                timestamp=timestamp_seconds,
            )
            message_queue.put_nowait(snapshot_msg)

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        trades = raw_message.get("data", [])
        for trade_data in trades:
            symbol = raw_message["arg"]["instId"]
            trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol)
            ts_ms = int(trade_data.get("ts", 0))
            trade_type = float(TradeType.BUY.value) if trade_data["side"] == "buy" else float(TradeType.SELL.value)
            message_content = {
                "trade_id": trade_data.get("tradeId", ""),
                "trading_pair": trading_pair,
                "trade_type": trade_type,
                "amount": trade_data["size"],
                "price": trade_data["price"],
            }
            trade_message = OrderBookMessage(
                message_type=OrderBookMessageType.TRADE,
                content=message_content,
                timestamp=ts_ms * 1e-3,
            )
            message_queue.put_nowait(trade_message)

    async def _parse_funding_info_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        data_list = raw_message.get("data", [])
        if not data_list:
            return
        entry = data_list[0]
        symbol = raw_message["arg"]["instId"]
        trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol)

        info_update = FundingInfoUpdate(trading_pair)
        if "indexPrice" in entry:
            info_update.index_price = Decimal(str(entry["indexPrice"]))
        if "markPrice" in entry:
            info_update.mark_price = Decimal(str(entry["markPrice"]))
        if "fundingRate" in entry:
            info_update.rate = Decimal(str(entry["fundingRate"]))
        if "nextFundingTime" in entry:
            info_update.next_funding_utc_timestamp = int(entry["nextFundingTime"]) // 1000
        message_queue.put_nowait(info_update)

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        product_type = await self._connector.product_type_associated_to_trading_pair(trading_pair)

        rest_assistant = await self._api_factory.get_rest_assistant()
        url = web_utils.get_rest_url_for_endpoint(endpoint=CONSTANTS.PUBLIC_ORDERBOOK_ENDPOINT)
        params = {
            "symbol": symbol,
            "productType": product_type,
            "limit": "50",
        }
        data = await rest_assistant.execute_request(
            url=url,
            throttler_limit_id=CONSTANTS.PUBLIC_ORDERBOOK_ENDPOINT,
            params=params,
            method=RESTMethod.GET,
        )
        snapshot_data = data["data"]
        timestamp_seconds = int(snapshot_data.get("ts", 0)) / 1e3

        bids, asks = self._get_bids_and_asks(snapshot_data)
        order_book_message_content = {
            "trading_pair": trading_pair,
            "update_id": 0,
            "bids": bids,
            "asks": asks,
        }
        snapshot_msg = OrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content=order_book_message_content,
            timestamp=timestamp_seconds,
        )
        return snapshot_msg

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        pass

    @staticmethod
    def _get_bids_and_asks(
        data: Dict[str, Any],
    ) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
        bids = [(float(row[0]), float(row[1])) for row in data.get("bids", [])]
        asks = [(float(row[0]), float(row[1])) for row in data.get("asks", [])]
        return bids, asks

    async def _connected_websocket_assistant(self) -> WSAssistant:
        pass  # unused

    async def _subscribe_channels(self, ws: WSAssistant):
        pass  # unused
