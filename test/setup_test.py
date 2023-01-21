import os
import sys

if sys.version_info >= (3, 8):
    from unittest import IsolatedAsyncioTestCase
else:
    from later.unittest.backport.async_case import IsolatedAsyncioTestCase

from masqott import Client, ReasonCode


def get_from_env(arg_name, default):
    return os.environ.get("MQTTTEST" + arg_name.upper(), default)


version_string = (
    f"{sys.version_info[0]}{sys.version_info[1]}_"
    f"{get_from_env('port', '1883')}")


class GetClientMixin:

    def create_client(self, client_id: str = "", **kwargs) -> Client:
        if client_id:
            client_id = f"{client_id}{version_string}"
        return Client(
            get_from_env("host", "localhost"),
            int(get_from_env("port", "1883")),
            client_id=client_id,
            **kwargs,
        )

    async def get_client(self, client_id: str = "", **kwargs) -> Client:
        client = self.create_client(client_id, **kwargs)
        await client.connect()
        return client


class BaseClientTestCase(GetClientMixin, IsolatedAsyncioTestCase):

    async def asyncSetUp(self) -> None:
        self._cn = await self.get_client("test_client")

    async def asyncTearDown(self) -> None:
        await self._cn.close()
