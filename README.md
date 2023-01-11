Masqott
=======

A Python asyncio client library for the MQTT protocol.

Very much alpha for now. Missing features:

* Auto reconnect
* Session state handling
* Probably more

Publish example:

```python
import asyncio
from masqott import AppMessage, Client, Qos

async def main():
    client = Client("localhost")
    await client.connect("client_id1", "user", "password")
    await client.publish(AppMessage("topic/12", "hello", qos=Qos.EXACTLY_ONCE))
    await client.disconnect()
        
asyncio.run(main())
```

Subscribe example:

```python
import asyncio
from masqott import Client, Qos, SubscriptionRequest

async def main():
    client = Client("localhost")
    await client.connect("client_id2", "user", "password")
    sub = await client.subscribe(
        SubscriptionRequest("topic/#", max_qos=Qos.EXACTLY_ONCE))
    msg = await client.get_message()
    print(msg.payload)
    await sub.unsubscribe()
    await client.disconnect()
        
asyncio.run(main())
```