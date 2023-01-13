import os
import sys

if sys.version_info >= (3, 8):
    from unittest import IsolatedAsyncioTestCase
else:
    from later.unittest.backport.async_case import IsolatedAsyncioTestCase

from masqott import Client, ReasonCode


def get_from_env(arg_name, default):
    return os.environ.get("MQTTTEST" + arg_name.upper(), default)


class BaseClientTestCase(IsolatedAsyncioTestCase):

    async def asyncSetUp(self) -> None:
        client = Client(
            get_from_env("host", "localhost"),
            int(get_from_env("port", "1883")),
            client_id="test_client"
        )
        await client.connect()
        self._cn = client

    async def asyncTearDown(self) -> None:
        await self._cn.disconnect()
