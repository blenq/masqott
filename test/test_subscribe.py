import sys

if sys.version_info >= (3, 8):
    from unittest import IsolatedAsyncioTestCase
else:
    from later.unittest.backport.async_case import IsolatedAsyncioTestCase

from masqott import (
    ReasonCode, AppMessage, MQTTException, Qos, SubscriptionRequest)

from .setup_test import BaseClientTestCase, version_string


class SubscribeTestCase(BaseClientTestCase):

    async def test_subscribe(self):
        sub = await self._cn.subscribe(f"test/{version_string}/#")
        await self._cn.publish((f"test/{version_string}", "first"))
        await self._cn.unsubscribe(f"test/{version_string}/#")
        await self._cn.publish((f"test/{version_string}", "second"))
        await self._cn.subscribe(f"test/{version_string}/#")
        await self._cn.publish((f"test/{version_string}", "third"))
        await self._cn.unsubscribe([f"test/{version_string}/#"])
        msg = await self._cn.get_message()
        self.assertEqual(msg.payload, "first")
        self.assertEqual(msg.subscription_id[0], sub.subscription_id)
        msg = await self._cn.get_message()
        self.assertEqual(msg.payload, "third")

    async def test_without_sub_id(self):
        # patch client
        self._cn._protocol._subscription_id_available = False
        sub = await self._cn.subscribe(f"test/{version_string}/#")
        self.assertIsNone(sub.subscription_id)
        await self._cn.publish((f"test/{version_string}", "first"))
        msg = await self._cn.get_message()
        self.assertIsNone(msg.subscription_id)

    async def test_sub_with_props(self):
        await self._cn.subscribe(
            SubscriptionRequest(f"test/{version_string}/#"),
            [("name", "value")])
        await self._cn.publish((f"test/{version_string}", "first"))
        msg = await self._cn.get_message()
        self.assertEqual(msg.payload, "first")

    async def test_empty_subscribe(self):
        with self.assertRaises(ValueError):
            await self._cn.subscribe("")

    async def test_empty_unsubscribe(self):
        with self.assertRaises(ValueError):
            await self._cn.unsubscribe("")
        with self.assertRaises(ValueError):
            await self._cn.unsubscribe(["test", ""])

    async def test_subscription_unsubscribe(self):
        sub = await self._cn.subscribe(f"test/{version_string}/#")
        await self._cn.publish((f"test/{version_string}", "first"))
        res = await sub.unsubscribe()
        self.assertIs(res, ReasonCode.SUCCESS)

        # unsubscribe from non-existing subscription
        res = await sub.unsubscribe()
        # mosquitto returns NO_SUBSCRIPTION_EXISTED, hivemq returns SUCCESS
        self.assertIn(res, (
            ReasonCode.NO_SUBSCRIPTION_EXISTED, ReasonCode.SUCCESS))

        await self._cn.publish(("test", "second"))
        await self._cn.subscribe(f"test/{version_string}/#")
        await self._cn.publish((f"test/{version_string}", "third"))
        msg = await self._cn.get_message()
        self.assertEqual(msg.payload, "first")
        msg = await self._cn.get_message()
        self.assertEqual(msg.payload, "third")

    async def test_will(self):
        await self._cn.subscribe(f"dead/{version_string}/#")
        cn2 = await self.get_client("client2", will_msg=AppMessage(
            f"dead/{version_string}/test", "I am dead",
            user_props=[("name", "value")]))
        cn2._protocol._transport.abort()
        await cn2.close()
        msg = await self._cn.get_message()
        self.assertEqual(msg.payload, "I am dead")
        self.assertEqual(msg.user_props, [("name", "value")])

    async def test_will_with_retain(self):
        cn2 = await self.get_client("client2", will_msg=AppMessage(
            f"dead/{version_string}/test", "I am dead", retain=True))
        cn2._protocol._transport.abort()
        await cn2.close()
        await self._cn.subscribe(f"dead/{version_string}/#")
        msg = await self._cn.get_message()
        self.assertEqual(msg.payload, "I am dead")
        # remove retained message
        await self._cn.publish(AppMessage(
            f"dead/{version_string}/test", "", retain=True,
            qos=Qos.AT_LEAST_ONCE))

    async def test_invalid_subscribe(self):
        with self.assertRaises(MQTTException) as ctx:
            await self._cn.subscribe(f"topic/{version_string}#")
        self.assertIn(
            ctx.exception.reason_code, (
                ReasonCode.MALFORMED_PACKET, ReasonCode.INVALID_TOPIC_FILTER))
        self.assertIsInstance(ctx.exception.message, str)
