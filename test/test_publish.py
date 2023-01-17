import asyncio

import masqott.base_protocol
from masqott import AppMessage, Qos, SubscriptionRequest

from .setup_test import BaseClientTestCase, version_string


class PublishTestCase(BaseClientTestCase):

    async def test_max_receive(self):
        await self._cn.subscribe(f"topic/{version_string}/#")
        num_msgs = 500
        coros = [
            self._cn.publish(AppMessage(
                f"topic/{version_string}/yo", "hoi", qos=Qos.AT_LEAST_ONCE))
            for i in range(num_msgs)
        ]
        await asyncio.gather(*coros)
        for i in range(num_msgs):
            await self._cn.get_message()
        self.assertEqual(self._cn._msq_queue.qsize(), 0)

    async def test_large_message(self):
        await self._cn.subscribe(f"topic/{version_string}/#")
        payload = "d" * 256
        await self._cn.publish((f"topic/{version_string}/test", payload))
        msg = await self._cn.get_message()
        self.assertEqual(payload, msg.payload)

    async def test_xlarge_message(self):
        await self._cn.subscribe(f"topic/{version_string}/#")
        payload = "d" * (
            masqott.base_protocol.BaseProtocol.STANDARD_BUF_SIZE + 1)
        await self._cn.publish((f"topic/{version_string}/test", payload))
        msg = await self._cn.get_message()
        self.assertEqual(payload, msg.payload)

    async def test_user_props(self):
        await self._cn.subscribe(f"topic/{version_string}/#")
        user_props = [
            ("name", "value1"), ("name", "value2"), ("key", "value")]
        msg = AppMessage(
            f"topic/{version_string}/test", "hello", user_props=user_props)
        await self._cn.publish(msg)
        msg = await self._cn.get_message()
        self.assertEqual(user_props, msg.user_props)

    async def test_correlation_data(self):
        await self._cn.subscribe(f"topic/{version_string}/#")
        correlation_data = b'hello'
        msg = AppMessage(
            f"topic/{version_string}/test", "hello",
            correlation_data=correlation_data)
        await self._cn.publish(msg)
        msg = await self._cn.get_message()
        self.assertEqual(correlation_data, msg.correlation_data)

    async def test_msg_expiry_interval_data(self):
        await self._cn.subscribe(f"topic/{version_string}/#")
        msg = AppMessage(
            f"topic/{version_string}/test", "hello", expiry_interval=300)
        await self._cn.publish(msg)
        msg = await self._cn.get_message()
        self.assertEqual(300, msg.expiry_interval)

    async def test_msg_qos(self):
        await self._cn.subscribe(
            SubscriptionRequest(
                f"topic/{version_string}/#", max_qos=Qos.EXACTLY_ONCE))
        await self._cn.publish(AppMessage(
            f"topic/{version_string}/test", "hello", qos=Qos.AT_MOST_ONCE))
        msg = await self._cn.get_message()
        self.assertEqual(Qos.AT_MOST_ONCE, msg.qos)
        await self._cn.publish(AppMessage(
            f"topic/{version_string}/test", "hello", qos=Qos.AT_LEAST_ONCE))
        msg = await self._cn.get_message()
        self.assertEqual(Qos.AT_LEAST_ONCE, msg.qos)
        await self._cn.publish(AppMessage(
            f"topic/{version_string}/test", "hello", qos=Qos.EXACTLY_ONCE))
        msg = await self._cn.get_message()
        self.assertEqual(Qos.EXACTLY_ONCE, msg.qos)

