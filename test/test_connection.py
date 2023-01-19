import sys

if sys.version_info >= (3, 8):
    from unittest import IsolatedAsyncioTestCase
else:
    from later.unittest.backport.async_case import IsolatedAsyncioTestCase

from .setup_test import BaseClientTestCase, version_string


class ConnCase(BaseClientTestCase):

    async def test_client_id(self):
        self.assertTrue(self._cn.client_id.startswith("test_client"))

    async def test_already_connected(self):
        with self.assertRaises(ValueError):
            await self._cn.connect()

    async def test_closed_publish(self):
        await self._cn.close()
        with self.assertRaises(ValueError):
            await self._cn.publish((f"topic/{version_string}", "message"))

    async def test_closed_unsubscribe(self):
        await self._cn.close()
        with self.assertRaises(ValueError):
            await self._cn.unsubscribe(f"topic/{version_string}/#")

    async def test_close_twice(self):
        await self._cn.close()
        await self._cn.close()

    async def test_user_pwd(self):
        cn = await self.get_client(user="user", password="password")
        await cn.close()
        cn = await self.get_client(user="user", password=b"password")
        await cn.close()

    async def test_conn_props(self):
        cn = await self.get_client(user_props=[("name", "value")])
        await cn.close()
