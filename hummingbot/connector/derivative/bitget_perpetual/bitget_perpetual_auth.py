import base64
import hashlib
import hmac
import time
from typing import Any, Dict, Optional

from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSRequest


class BitgetPerpetualAuth(AuthBase):

    def __init__(
        self,
        api_key: str,
        secret_key: str,
        passphrase: str,
        time_provider: TimeSynchronizer,
    ):
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase
        self.time_provider = time_provider

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        timestamp = str(int(time.time() * 1000))

        if request.method == RESTMethod.GET:
            path = request.url.split(".com")[-1]
            if request.params:
                from urllib.parse import urlencode
                path = path + "?" + urlencode(request.params)
            body_str = ""
        else:
            path = request.url.split(".com")[-1]
            body_str = request.data if request.data else ""

        message = timestamp + request.method.value + path + body_str
        signature = self._sign(message)

        headers = request.headers or {}
        headers["ACCESS-KEY"] = self.api_key
        headers["ACCESS-SIGN"] = signature
        headers["ACCESS-TIMESTAMP"] = timestamp
        headers["ACCESS-PASSPHRASE"] = self.passphrase
        headers["Content-Type"] = "application/json"
        request.headers = headers

        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        return request

    def get_ws_auth_payload(self) -> Dict[str, Any]:
        timestamp = str(int(time.time() * 1000))
        message = timestamp + "GET" + "/user/verify"
        signature = self._sign(message)

        auth_message = {
            "op": "login",
            "args": [
                {
                    "apiKey": self.api_key,
                    "passphrase": self.passphrase,
                    "timestamp": timestamp,
                    "sign": signature,
                }
            ],
        }
        return auth_message

    def _sign(self, message: str) -> str:
        mac = hmac.new(
            bytes(self.secret_key, encoding="utf-8"),
            bytes(message, encoding="utf-8"),
            digestmod=hashlib.sha256,
        )
        return base64.b64encode(mac.digest()).decode()

    def _time(self):
        return time.time()
