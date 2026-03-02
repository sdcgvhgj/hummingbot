from typing import Any, Callable, Dict, List, Optional

from hummingbot.connector.derivative.bitget_perpetual import bitget_perpetual_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.connector.utils import TimeSynchronizerRESTPreProcessor
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest
from hummingbot.core.web_assistant.rest_pre_processors import RESTPreProcessorBase
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


REST_URL = f"https://{CONSTANTS.REST_SUBDOMAIN}.{CONSTANTS.DEFAULT_DOMAIN}"
WSS_PUBLIC_URL = f"wss://{CONSTANTS.WSS_SUBDOMAIN}.{CONSTANTS.DEFAULT_DOMAIN}{CONSTANTS.WSS_PUBLIC_ENDPOINT}"
WSS_PRIVATE_URL = f"wss://{CONSTANTS.WSS_SUBDOMAIN}.{CONSTANTS.DEFAULT_DOMAIN}{CONSTANTS.WSS_PRIVATE_ENDPOINT}"


class HeadersContentRESTPreProcessor(RESTPreProcessorBase):
    async def pre_process(self, request: RESTRequest) -> RESTRequest:
        request.headers = request.headers or {}
        request.headers["Content-Type"] = "application/json"
        return request


def build_api_factory(
    throttler: Optional[AsyncThrottler] = None,
    time_synchronizer: Optional[TimeSynchronizer] = None,
    time_provider: Optional[Callable] = None,
    auth: Optional[AuthBase] = None,
) -> WebAssistantsFactory:
    throttler = throttler or create_throttler()
    time_synchronizer = time_synchronizer or TimeSynchronizer()
    time_provider = time_provider or (lambda: get_current_server_time(throttler=throttler))
    api_factory = WebAssistantsFactory(
        throttler=throttler,
        auth=auth,
        rest_pre_processors=[
            TimeSynchronizerRESTPreProcessor(synchronizer=time_synchronizer, time_provider=time_provider),
            HeadersContentRESTPreProcessor(),
        ],
    )
    return api_factory


def create_throttler() -> AsyncThrottler:
    return AsyncThrottler(CONSTANTS.RATE_LIMITS)


async def get_current_server_time(
    throttler: Optional[AsyncThrottler] = None,
    domain: str = CONSTANTS.DEFAULT_DOMAIN,
) -> float:
    throttler = throttler or create_throttler()
    api_factory = build_api_factory_without_time_synchronizer_pre_processor(throttler=throttler)
    rest_assistant = await api_factory.get_rest_assistant()
    url = public_rest_url(path_url=CONSTANTS.PUBLIC_TIME_ENDPOINT)
    response = await rest_assistant.execute_request(
        url=url,
        throttler_limit_id=CONSTANTS.PUBLIC_TIME_ENDPOINT,
        method=RESTMethod.GET,
    )
    server_time = float(response["data"]["serverTime"])
    return server_time


def public_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    return REST_URL + path_url


def private_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    return REST_URL + path_url


def get_ws_public_url(domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    return WSS_PUBLIC_URL


def get_ws_private_url(domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    return WSS_PRIVATE_URL


def build_api_factory_without_time_synchronizer_pre_processor(
    throttler: AsyncThrottler,
) -> WebAssistantsFactory:
    api_factory = WebAssistantsFactory(throttler=throttler)
    return api_factory


def endpoint_from_message(message: Dict[str, Any]) -> Optional[str]:
    endpoint = None
    if isinstance(message, dict):
        arg = message.get("arg", {})
        if isinstance(arg, dict):
            endpoint = arg.get("channel")
    return endpoint


def payload_from_message(message: Dict[str, Any]) -> List[Dict[str, Any]]:
    payload = message.get("data", message)
    return payload
