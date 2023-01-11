import sys

if sys.version_info >= (3, 8):
    from unittest import IsolatedAsyncioTestCase
else:
    from later.unittest.backport.async_case import IsolatedAsyncioTestCase

from masqott import Client, ReasonCode

from .setup_test import BaseClientTestCase


class SubscribeTestCase(BaseClientTestCase):

    async def test_subscribe(self):
        await self._cn.subscribe("test/#")
        await self._cn.publish(("test", "first"))
        await self._cn.unsubscribe("test/#")
        await self._cn.publish(("test", "second"))
        await self._cn.subscribe("test/#")
        await self._cn.publish(("test", "third"))
        msg = await self._cn.get_message()
        self.assertEqual(msg.payload, "first")
        msg = await self._cn.get_message()
        self.assertEqual(msg.payload, "third")

    async def test_subscription_unsubscribe(self):
        sub = await self._cn.subscribe("test/#")
        await self._cn.publish(("test", "first"))
        res = await sub.unsubscribe()
        self.assertIs(res, ReasonCode.SUCCESS)

        # unsubscribe from non-existing subscription
        res = await sub.unsubscribe()
        # mosquitto returns NO_SUBSCRIPTION_EXISTED, hivemq returns SUCCESS
        self.assertIn(res, (
            ReasonCode.NO_SUBSCRIPTION_EXISTED, ReasonCode.SUCCESS))

        await self._cn.publish(("test", "second"))
        await self._cn.subscribe("test/#")
        await self._cn.publish(("test", "third"))
        msg = await self._cn.get_message()
        self.assertEqual(msg.payload, "first")
        msg = await self._cn.get_message()
        self.assertEqual(msg.payload, "third")
