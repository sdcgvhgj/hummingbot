import asyncio
import time
from typing import List, Optional

from hummingbot.connector.derivative.bitget_perpetual import (
    bitget_perpetual_constants as CONSTANTS,
    bitget_perpetual_web_utils as web_utils,
)
from hummingbot.connector.derivative.bitget_perpetual.bitget_perpetual_auth import BitgetPerpetualAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger


class BitgetPerpetualUserStreamDataSource(UserStreamTrackerDataSource):
    _logger: Optional[HummingbotLogger] = None

    def __init__(
        self,
        auth: BitgetPerpetualAuth,
        trading_pairs: List[str],
        connector,
        api_factory: WebAssistantsFactory,
    ):
        super().__init__()
        self._auth = auth
        self._trading_pairs = trading_pairs
        self._connector = connector
        self._api_factory = api_factory
        self._ws_assistant: Optional[WSAssistant] = None

    @property
    def last_recv_time(self) -> float:
        if self._ws_assistant is not None:
            return self._ws_assistant.last_recv_time
        return 0.0

    async def listen_for_user_stream(self, output: asyncio.Queue):
        ws: Optional[WSAssistant] = None
        while True:
            try:
                ws = await self._get_connected_websocket_assistant()
                await self._authenticate_connection(ws)
                await self._subscribe_channels(ws)
                self._last_ws_message_sent_timestamp = self._time()
                while True:
                    try:
                        seconds_until_next_ping = (
                            CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL
                            - (self._time() - self._last_ws_message_sent_timestamp)
                        )
                        await asyncio.wait_for(
                            self._process_ws_messages(ws=ws, output=output),
                            timeout=seconds_until_next_ping,
                        )
                    except asyncio.TimeoutError:
                        await self._ping_server(ws)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception(
                    "Unexpected error while listening to user stream. Retrying after 5 seconds..."
                )
            finally:
                ws and await ws.disconnect()
                await self._sleep(5)

    async def _get_connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(
            ws_url=web_utils.get_ws_private_url(),
            ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL,
        )
        self._ws_assistant = ws
        return ws

    async def _authenticate_connection(self, ws: WSAssistant):
        auth_payload = self._auth.get_ws_auth_payload()
        request = WSJSONRequest(payload=auth_payload)
        await ws.send(request)
        # Wait for auth response
        async for ws_response in ws.iter_messages():
            data = ws_response.data
            if isinstance(data, dict) and data.get("event") == "login":
                if data.get("code") == "0":
                    self.logger().info("Bitget private channel authentication success.")
                else:
                    error_msg = f"Bitget private channel authentication failed: {data.get('msg', '')}"
                    self.logger().error(error_msg)
                    raise IOError(error_msg)
                break

    async def _subscribe_channels(self, ws: WSAssistant):
        try:
            for trading_pair in self._trading_pairs:
                symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair)
                product_type = await self._connector.product_type_associated_to_trading_pair(trading_pair)
                inst_type = self._product_type_to_inst_type(product_type)

                subscribe_positions = {
                    "op": "subscribe",
                    "args": [{"instType": inst_type, "channel": CONSTANTS.WS_POSITIONS_ENDPOINT, "instId": "default"}],
                }
                subscribe_orders = {
                    "op": "subscribe",
                    "args": [{"instType": inst_type, "channel": CONSTANTS.WS_ORDERS_ENDPOINT, "instId": symbol}],
                }
                subscribe_account = {
                    "op": "subscribe",
                    "args": [{"instType": inst_type, "channel": CONSTANTS.WS_ACCOUNT_ENDPOINT, "coin": "default"}],
                }

                await ws.send(WSJSONRequest(payload=subscribe_positions))
                await ws.send(WSJSONRequest(payload=subscribe_orders))
                await ws.send(WSJSONRequest(payload=subscribe_account))

            self.logger().info("Subscribed to private positions, orders and account channels")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to private channels...",
                exc_info=True,
            )
            raise

    @staticmethod
    def _product_type_to_inst_type(product_type: str) -> str:
        mapping = {
            CONSTANTS.USDT_PRODUCT_TYPE: "USDT-FUTURES",
            CONSTANTS.USDC_PRODUCT_TYPE: "USDC-FUTURES",
            CONSTANTS.USD_PRODUCT_TYPE: "COIN-FUTURES",
        }
        return mapping.get(product_type, "USDT-FUTURES")

    async def _process_ws_messages(self, ws: WSAssistant, output: asyncio.Queue):
        async for ws_response in ws.iter_messages():
            data = ws_response.data
            if isinstance(data, str):
                if data == CONSTANTS.PUBLIC_WS_PONG_RESPONSE:
                    continue
            if isinstance(data, dict):
                if data.get("event") in ("subscribe", "login"):
                    continue
                if "arg" in data and "data" in data:
                    output.put_nowait(data)

    async def _ping_server(self, ws: WSAssistant):
        ping_time = self._time()
        ping_request = WSJSONRequest(payload=CONSTANTS.PUBLIC_WS_PING_REQUEST)
        await ws.send(request=ping_request)
        self._last_ws_message_sent_timestamp = ping_time

    async def _connected_websocket_assistant(self) -> WSAssistant:
        pass  # unused

    def _time(self):
        return time.time()
