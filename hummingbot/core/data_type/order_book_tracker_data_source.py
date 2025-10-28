import asyncio
import logging
import time
import os
from abc import ABCMeta, abstractmethod
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional

from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger


class OrderBookTrackerDataSource(metaclass=ABCMeta):
    FULL_ORDER_BOOK_RESET_DELTA_SECONDS = 60 * 60
    # FULL_ORDER_BOOK_RESET_DELTA_SECONDS = 1 * 60 # for debug

    _logger: Optional[HummingbotLogger] = None

    def __init__(self, trading_pairs: List[str]):
        self._trade_messages_queue_key = "trade"
        self._diff_messages_queue_key = "order_book_diff"
        self._snapshot_messages_queue_key = "order_book_snapshot"

        self._trading_pairs: List[str] = trading_pairs
        self._order_book_create_function = lambda: OrderBook()
        self._message_queue: Dict[str, asyncio.Queue] = defaultdict(asyncio.Queue)

        # Lightweight backlog diagnostics (disabled by default). Enable with HB_OB_QUEUE_MONITOR=1
        self._monitor_enabled: bool = str(os.getenv("HB_OB_QUEUE_MONITOR", "0")).lower() in ("1", "true", "yes")
        self._mon_last_log_ts: float = time.time()
        self._mon_interval_sec: float = float(os.getenv("HB_OB_QUEUE_MONITOR_INTERVAL", "5"))
        self._mon_counts: Dict[str, int] = {
            self._trade_messages_queue_key: 0,
            self._diff_messages_queue_key: 0,
            self._snapshot_messages_queue_key: 0,
            "unknown": 0,
        }

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(HummingbotLogger.logger_name_for_class(cls))
        return cls._logger

    @property
    def order_book_create_function(self) -> Callable[[], OrderBook]:
        return self._order_book_create_function

    @order_book_create_function.setter
    def order_book_create_function(self, func: Callable[[], OrderBook]):
        self._order_book_create_function = func

    @abstractmethod
    async def get_last_traded_prices(self, trading_pairs: List[str], domain: Optional[str] = None) -> Dict[str, float]:
        """
        Return a dictionary the trading_pair as key and the current price as value for each trading pair passed as
        parameter.
        This method is required by the order book tracker, to get the last traded prices when no new public trades
        are notified by the exchange.

        :param trading_pairs: list of trading pairs to get the prices for
        :param domain: which domain we are connecting to

        :return: Dictionary of associations between token pair and its latest price
        """
        raise NotImplementedError

    async def get_new_order_book(self, trading_pair: str) -> OrderBook:
        """
        Creates a local instance of the exchange order book for a particular trading pair

        :param trading_pair: the trading pair for which the order book has to be retrieved

        :return: a local copy of the current order book in the exchange
        """
        snapshot_msg: OrderBookMessage = await self._order_book_snapshot(trading_pair=trading_pair)
        order_book: OrderBook = self.order_book_create_function()
        self.logger().debug(f"apply first snapshot for {trading_pair}, update_id: {snapshot_msg.update_id}")
        order_book.apply_snapshot(snapshot_msg.bids, snapshot_msg.asks, snapshot_msg.update_id)
        return order_book

    async def listen_for_subscriptions(self):
        """
        Connects to the trade events and order diffs websocket endpoints and listens to the messages sent by the
        exchange. Each message is stored in its own queue.
        """
        ws: Optional[WSAssistant] = None
        while True:
            try:
                ws: WSAssistant = await self._connected_websocket_assistant()
                await self._subscribe_channels(ws)
                await self._process_websocket_messages(websocket_assistant=ws)
            except asyncio.CancelledError:
                raise
            except ConnectionError as connection_exception:
                self.logger().warning(f"The websocket connection was closed ({connection_exception})")
            except Exception:
                self.logger().exception(
                    "Unexpected error occurred when listening to order book streams. Retrying in 5 seconds...",
                )
                await self._sleep(1.0)
            finally:
                await self._on_order_stream_interruption(websocket_assistant=ws)

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """
        Reads the order diffs events queue. For each event creates a diff message instance and adds it to the
        output queue

        :param ev_loop: the event loop the method will run in
        :param output: a queue to add the created diff messages
        """
        message_queue = self._message_queue[self._diff_messages_queue_key]
        while True:
            try:
                diff_event = await message_queue.get()
                # self.logger().debug(f"order-book-diffs loop, get diff message")
                await self._parse_order_book_diff_message(raw_message=diff_event, message_queue=output)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error when processing public order book updates from exchange")

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """
        Reads the order snapshot events queue. For each event it creates a snapshot message instance and adds it to the
        output queue.
        This method also request the full order book content from the exchange using HTTP requests if it does not
        receive events during one hour.

        :param ev_loop: the event loop the method will run in
        :param output: a queue to add the created snapshot messages
        """
        message_queue = self._message_queue[self._snapshot_messages_queue_key]
        while True:
            try:
                try:
                    snapshot_event = await asyncio.wait_for(message_queue.get(),
                                                            timeout=self.FULL_ORDER_BOOK_RESET_DELTA_SECONDS)
                    self.logger().debug("order-book-snapshots loop, get snapshot_event")
                    await self._parse_order_book_snapshot_message(raw_message=snapshot_event, message_queue=output)
                except asyncio.TimeoutError:
                    self.logger().debug("order-book-snapshots loop timeout, request orderbook snapshots")
                    await self._request_order_book_snapshots(output=output)
                    self.logger().debug("order-book-snapshots loop, request orderbook snapshots done")
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error when processing public order book snapshots from exchange")
                await self._sleep(1.0)

    async def listen_for_trades(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """
        Reads the trade events queue. For each event creates a trade message instance and adds it to the output queue

        :param ev_loop: the event loop the method will run in
        :param output: a queue to add the created trade messages
        """
        message_queue = self._message_queue[self._trade_messages_queue_key]
        while True:
            try:
                trade_event = await message_queue.get()
                await self._parse_trade_message(raw_message=trade_event, message_queue=output)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error when processing public trade updates from exchange")

    async def _request_order_book_snapshots(self, output: asyncio.Queue):
        for trading_pair in self._trading_pairs:
            try:
                snapshot = await self._order_book_snapshot(trading_pair=trading_pair)
                output.put_nowait(snapshot)
                self.logger().debug(f"order-book-snapshots loop, put snapshot for {trading_pair}")
            except Exception:
                self.logger().exception(f"Unexpected error fetching order book snapshot for {trading_pair}.")
                raise

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """
        Create an instance of OrderBookMessage of type OrderBookMessageType.TRADE

        :param raw_message: the JSON dictionary of the public trade event
        :param message_queue: queue where the parsed messages should be stored in
        """
        raise NotImplementedError

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """
        Create an instance of OrderBookMessage of type OrderBookMessageType.DIFF

        :param raw_message: the JSON dictionary of the public trade event
        :param message_queue: queue where the parsed messages should be stored in
        """
        raise NotImplementedError

    async def _parse_order_book_snapshot_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """
        Create an instance of OrderBookMessage of type OrderBookMessageType.SNAPSHOT

        :param raw_message: the JSON dictionary of the public trade event
        :param message_queue: queue where the parsed messages should be stored in
        """
        raise NotImplementedError

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        raise NotImplementedError

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """
        Creates an instance of WSAssistant connected to the exchange

        :return: an instance of WSAssistant connected to the exchange
        """
        raise NotImplementedError

    async def _subscribe_channels(self, ws: WSAssistant):
        """
        Subscribes to the trade events and diff orders events through the provided websocket connection.

        :param ws: the websocket assistant used to connect to the exchange
        """
        raise NotImplementedError

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        """
        Identifies the channel for a particular event message. Used to find the correct queue to add the message in

        :param event_message: the event received through the websocket connection

        :return: the message channel
        """
        raise NotImplementedError

    async def _process_message_for_unknown_channel(
        self, event_message: Dict[str, Any], websocket_assistant: WSAssistant
    ):
        """
        Processes a message coming from a not identified channel.
        Does nothing by default but allows subclasses to reimplement

        :param event_message: the event received through the websocket connection
        :param websocket_assistant: the websocket connection to use to interact with the exchange
        """
        pass

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant):
        async for ws_response in websocket_assistant.iter_messages():
            data: Dict[str, Any] = ws_response.data
            if data is not None:  # data will be None when the websocket is disconnected
                channel: str = self._channel_originating_message(event_message=data)
                valid_channels = self._get_messages_queue_keys()
                if channel in valid_channels:
                    self._message_queue[channel].put_nowait(data)
                    self._record_enqueue(channel)
                else:
                    self._record_enqueue("unknown")
                    await self._process_message_for_unknown_channel(
                        event_message=data, websocket_assistant=websocket_assistant
                    )
                self._maybe_log_queue_status()

    def _get_messages_queue_keys(self) -> List[str]:
        return [self._snapshot_messages_queue_key, self._diff_messages_queue_key, self._trade_messages_queue_key]

    async def _on_order_stream_interruption(self, websocket_assistant: Optional[WSAssistant] = None):
        websocket_assistant and await websocket_assistant.disconnect()

    async def _sleep(self, delay):
        """
        Function added only to facilitate patching the sleep in unit tests without affecting the asyncio module
        """
        await asyncio.sleep(delay)

    def _time(self):
        return time.time()

    # --------------------
    # Backlog diagnostics helpers
    # --------------------
    def _record_enqueue(self, channel: str):
        if not self._monitor_enabled:
            return
        try:
            self._mon_counts[channel] = self._mon_counts.get(channel, 0) + 1
        except Exception:
            pass

    def _maybe_log_queue_status(self):
        if not self._monitor_enabled:
            return
        now = time.time()
        if now - self._mon_last_log_ts < self._mon_interval_sec:
            return
        self._mon_last_log_ts = now

        try:
            # Queue sizes (length)
            lens: Dict[str, int] = {}
            for k in self._get_messages_queue_keys():
                q = self._message_queue.get(k)
                lens[k] = q.qsize() if q is not None else 0

            # Rates since last log
            counts = self._mon_counts
            rate_trade = counts.get(self._trade_messages_queue_key, 0) / self._mon_interval_sec
            rate_diff = counts.get(self._diff_messages_queue_key, 0) / self._mon_interval_sec
            rate_snap = counts.get(self._snapshot_messages_queue_key, 0) / self._mon_interval_sec
            rate_unknown = counts.get("unknown", 0) / self._mon_interval_sec

            self.logger().info(
                f"[ob-backlog] domain={self._domain} lens trade={lens.get(self._trade_messages_queue_key,0)} "
                f"diff={lens.get(self._diff_messages_queue_key,0)} snap={lens.get(self._snapshot_messages_queue_key,0)} | "
                f"rates t={rate_trade:.1f}/s d={rate_diff:.1f}/s s={rate_snap:.3f}/s u={rate_unknown:.3f}/s"
            )

            if self._funding_info_messages_queue_key is not None and lens.get(self._funding_info_messages_queue_key, 0) > 0:
                self.logger().info(f"[ob-backlog] domain={self._domain} lens funding_info={lens.get(self._funding_info_messages_queue_key,0)}")

            # reset window
            self._mon_counts = {
                self._trade_messages_queue_key: 0,
                self._diff_messages_queue_key: 0,
                self._snapshot_messages_queue_key: 0,
                "unknown": 0,
            }
        except Exception:
            pass
