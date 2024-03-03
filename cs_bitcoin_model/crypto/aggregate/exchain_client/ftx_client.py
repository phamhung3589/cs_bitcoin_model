import time
import urllib.parse
from typing import Optional, Dict, Any, List

from requests import Request, Session, Response
import hmac

from cs_bitcoin_model.crypto.utils.cs_config import CSConfig


class FtxClient:
    _ENDPOINT = 'https://ftx.com/api/'

    def __init__(self, subaccount_name=None) -> None:
        parser = CSConfig("private_product", self.__class__.__name__, "config")
        self._session = Session()
        self._api_key = parser.read_parameter("api")
        self._api_secret = parser.read_parameter("api_secret")
        self._subaccount_name = subaccount_name

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        return self._request('GET', path, params=params)

    def _post(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        return self._request('POST', path, json=params)

    def _delete(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        return self._request('DELETE', path, json=params)

    def _request(self, method: str, path: str, **kwargs) -> Any:
        request = Request(method, self._ENDPOINT + path, **kwargs)
        self._sign_request(request)
        response = self._session.send(request.prepare())
        return self._process_response(response)

    def _sign_request(self, request: Request) -> None:
        ts = int(time.time() * 1000)
        prepared = request.prepare()
        signature_payload = f'{ts}{prepared.method}{prepared.path_url}'.encode()
        if prepared.body:
            signature_payload += prepared.body
        signature = hmac.new(self._api_secret.encode(), signature_payload, 'sha256').hexdigest()
        request.headers['FTX-KEY'] = self._api_key
        request.headers['FTX-SIGN'] = signature
        request.headers['FTX-TS'] = str(ts)
        if self._subaccount_name:
            request.headers['FTX-SUBACCOUNT'] = urllib.parse.quote(self._subaccount_name)

    def _process_response(self, response: Response) -> Any:
        try:
            data = response.json()
        except ValueError:
            response.raise_for_status()
            raise
        else:
            if not data['success']:
                raise Exception(data['error'])
            return data['result']

    def get_stakes(self) -> List[dict]:
        return self._get('staking/stakes')

    def get_staking_rewards(self, start_time: float = None, end_time: float = None) -> List[dict]:
        return self._get('staking/staking_rewards', {
            'start_time': start_time,
            'end_time': end_time
        })

    def get_staking_balances(self) -> List[dict]:
        return self._get('staking/balances')

from jose import JWTError, jwt
import base64
def check_env():
    token = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxNSIsImlhdCI6MTY1NjY4NDA1OCwiZXhwIjoxNzQzMDg0MDU4fQ.3o1IrAfRQIVFcartLSyeteoP0yIOTtz9sCgqpaYvKiM"
    key = "hzFSok2hC4NVaYePMpOF0oZKpiIq8OcDY3ixGESyckhy2u2FzuEeO3U7wE59u7DP"
    algorithm = "HS256"
    payloads = jwt.decode(token=token, key=base64.b64decode(key),
                          algorithms=[algorithm])
    print(payloads)


if __name__ == "__main__":
    # ftx_client = FtxClient()
    # response = ftx_client.get_staking_balances()
    # print(response)
    check_env()
