import asyncio
import sys
from unittest.mock import patch, Mock

import masqott.base_protocol

if sys.version_info >= (3, 8):
    from unittest import IsolatedAsyncioTestCase
else:
    from later.unittest.backport.async_case import IsolatedAsyncioTestCase

from .setup_test import BaseClientTestCase, version_string, GetClientMixin

from masqott import AppMessage, PayloadFormat, MalformedPacket, ProtocolError
from masqott.base_protocol import (
    PacketType, PropertyID, PropertyInfo, UnPacker, Packer)


class BasicCase(GetClientMixin, IsolatedAsyncioTestCase):

    async def test_response_timeout_arg(self):
        cn = await self.get_client(response_timeout=10)
        await cn.subscribe(f"test/{version_string}/#")
        await cn.publish((f"test/{version_string}", "first"))
        msg = await cn.get_message()
        self.assertEqual(msg.payload, "first")
        await cn.close()

    async def test_invalid_response_timeout_arg(self):
        with self.assertRaises(ValueError):
            await self.get_client(response_timeout="-10")

    async def test_ping(self):
        cn = await self.get_client(keep_alive=2)
        await asyncio.sleep(0.5)
        await cn.publish((f"topic/{version_string}", "hello"))
        await asyncio.sleep(2.5)
        self.assertLess(cn._protocol._loop.time() - cn._protocol._last_sent, 1)
        await cn.close()

    async def test_invalid_will_topic(self):
        def write2(self, val):
            self.write_byte(2)
        Packer.write2 = write2

        props = masqott.base_protocol.properties
        old_info = props[PropertyID.PAYLOAD_FORMAT_INDICATOR]
        props[PropertyID.PAYLOAD_FORMAT_INDICATOR] = PropertyInfo(
            UnPacker.read_format_indicator, Packer.write2, True,
            PayloadFormat.UNSPECIFIED, {PacketType.PUBLISH, PacketType.WILL},
        )
        with self.assertRaises((MalformedPacket, ValueError)):
            await self.get_client(
                will_msg=AppMessage(f"topic/{version_string}", "test"))
        props[PropertyID.PAYLOAD_FORMAT_INDICATOR] = old_info

    async def test_invalid_connack_flags(self):
        with patch.object(
                masqott.base_protocol.UnPacker, "read_byte", new=Mock(
                    return_value=2)):
            with self.assertRaises(MalformedPacket):
                await self.get_client()


class ConnCase(BaseClientTestCase):

    async def test_client_id(self):
        self.assertTrue(self._cn.client_id.startswith("test_client"))

    async def test_already_connected(self):
        with self.assertRaises(ValueError):
            await self._cn.connect()

    async def test_unexpected_connack(self):
        buf = self._cn._protocol.get_buffer(-1)
        # connack message
        buf[:5] = b"\x20\x03\0\0\0"
        self._cn._protocol.buffer_updated(5)
        self.assertTrue(self._cn._protocol._transport.is_closing())

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
