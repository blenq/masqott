import asyncio

from masqott import AppMessage, Qos

from .setup_test import BaseClientTestCase


class PublishTestCase(BaseClientTestCase):

    async def test_max_receive(self):
        await self._cn.subscribe("topic/#")
        num_msgs = 500
        coros = [
            self._cn.publish(
                AppMessage("topic/yo", "hoi", qos=Qos.AT_LEAST_ONCE))
            for i in range(num_msgs)
        ]
        await asyncio.gather(*coros)
        for i in range(num_msgs):
            await self._cn.get_message()
        self.assertEqual(self._cn._msq_queue.qsize(), 0)
