# SPDX-License-Identifier: BSD-2-Clause

""" Asyncio MQTT client implementation"""

import asyncio
import typing
from codecs import decode
from collections import OrderedDict
from dataclasses import dataclass
from datetime import timedelta
import enum
import socket
from ssl import SSLContext
import sys


from .base_protocol import (
    BaseProtocol, PayloadFormat, Qos, MQTTVersion, PropertyID,
    PacketType, MalformedPacket, MessagePacker, UnPacker, RetainHandling,
    ReasonCode, MQTTException, ProtocolError, get_mqtt_ex, verify_reason_code)


class ClientStatus(enum.IntEnum):
    """ ClientStatus """
    CLOSED = 0
    TCP_CONNECTED = 1
    CONNECTING = 2
    CONNECTED = 3
    DISCONNECTING = 4
    CLOSING = 5


UserProps = typing.List[typing.Tuple[str, str]]


@dataclass
class SubscriptionRequest:
    """ Subscription request """
    topic_filter: str
    max_qos: Qos = Qos.AT_MOST_ONCE
    no_local: bool = False
    retain_as_published: bool = False
    retain_handling: RetainHandling = RetainHandling.ALWAYS

    def __post_init__(self) -> None:
        self.topic_filter = str(self.topic_filter)
        if not self.topic_filter:
            raise ValueError("Subscription topic filter can not be empty.")
        self.max_qos = Qos(self.max_qos)
        self.no_local = bool(self.no_local)
        self.retain_as_published = bool(self.retain_as_published)
        self.retain_handling = RetainHandling(self.retain_handling)


class Subscription:
    """ Subscription """

    # pylint: disable-next=too-many-arguments
    def __init__(
            self,
            topic_filter: str,
            qos: Qos,
            subscription_id: typing.Optional[int],
            no_local: bool,
            retain_as_published: bool,
            retain_handling: RetainHandling,
            client: 'Client',
    ) -> None:
        self._topic_filter = topic_filter
        self._qos = qos
        self._subscription_id = subscription_id
        self._no_local = no_local
        self._retain_as_published = retain_as_published
        self._retain_handling = retain_handling
        self._client = client

    async def unsubscribe(self) -> ReasonCode:
        """ Unsubscribes from the topic. """
        return await self._client.unsubscribe(self._topic_filter)

    @property
    def topic_filter(self) -> str:
        """ The topic filter. """
        return self._topic_filter

    @property
    def qos(self) -> Qos:
        """ Qos of the service. """
        return self._qos

    @property
    def subscription_id(self) -> typing.Optional[int]:
        """ The subscription id if set. """
        return self._subscription_id

    @property
    def no_local(self) -> bool:
        """ Indicates if messages originating from itself should be returned
        by the server.
        """
        return self._no_local

    @property
    def retain_as_published(self) -> bool:
        """ Indicates if the retain flag of an application message should be
        propagated to the client.
        """
        return self._retain_as_published

    @property
    def retain_handling(self) -> RetainHandling:
        """ The retain behaviour option of the subscription. """
        return self._retain_handling


# pylint: disable-next=too-many-instance-attributes
class AppMessage:
    """ An application message. """

    # pylint: disable-next=too-many-arguments
    def __init__(
            self,
            topic: str,
            payload: typing.Union[bytes, memoryview, bytearray, str],
            payload_format: typing.Optional[PayloadFormat] = None,
            qos: Qos = Qos.AT_MOST_ONCE,
            expiry_interval: typing.Optional[
                typing.Union[int, timedelta]] = None,
            response_topic: typing.Optional[str] = None,
            correlation_data: typing.Optional[bytes] = None,
            user_props: typing.Optional[UserProps] = None,
            subscription_id: typing.Optional[int] = None,
            content_type: typing.Optional[str] = None,
            retain: bool = False,
            duplicate: bool = False,
    ) -> None:
        topic = str(topic)
        if topic == "":
            raise ProtocolError("Topic can not be empty.")
        self._topic = topic
        if isinstance(payload, str):
            raw_payload = payload.encode()
            if payload_format is None:
                payload_format = PayloadFormat.TEXT
        else:
            raw_payload = payload
            if payload_format is PayloadFormat.TEXT:
                try:
                    payload = decode(payload)
                except UnicodeError as ex:
                    raise get_mqtt_ex(
                        ReasonCode.INVALID_PAYLOAD_FORMAT) from ex
            else:
                payload_format = PayloadFormat.UNSPECIFIED
        self._payload = payload
        self._raw_payload = raw_payload
        self._payload_format = payload_format
        self._qos = Qos(qos)
        self._retain = retain
        if isinstance(expiry_interval, timedelta):
            expiry_interval = (
                expiry_interval.days * 86400 + expiry_interval.seconds +
                bool(expiry_interval.microseconds))
        self._expiry_interval = expiry_interval
        self._response_topic = response_topic
        self._correlation_data = correlation_data
        if user_props is not None:
            user_props = list(user_props)
        self._user_props = user_props
        self._subscription_id = subscription_id
        self._content_type = content_type
        self._duplicate = duplicate

    @property
    def topic(self) -> str:
        """ The topic. """
        return self._topic

    @property
    def payload(self) -> typing.Union[str, bytes]:
        """ The payload of the message. If the payload format is TEXT it
        returns a string, otherwise a bytes object.
        """
        return self._payload

    @property
    def raw_payload(self) -> bytes:
        """ The binary message payload. """
        return self._raw_payload

    @property
    def payload_format(self) -> PayloadFormat:
        """ The payload format. """
        return self._payload_format

    @property
    def qos(self) -> Qos:
        """ The qos of the message. """
        return self._qos

    @property
    def retain(self) -> bool:
        """ Indicates if the message is a retained message for the topic. """
        return self._retain

    @property
    def expiry_interval(self) -> typing.Optional[int]:
        """ The expiry interval in seconds. """
        return self._expiry_interval

    @property
    def response_topic(self) -> typing.Optional[str]:
        """ The associated response topic. """
        return self._response_topic

    @property
    def correlation_data(self) -> typing.Optional[bytes]:
        """ Correlation data of the message. """
        return self._correlation_data

    @property
    def user_props(self) -> typing.Optional[UserProps]:
        """ The user properties of the message. """
        return self._user_props

    @property
    def subscription_id(self) -> typing.Optional[int]:
        """ The subscription identifier of the message if set."""
        return self._subscription_id

    @property
    def content_type(self) -> typing.Optional[str]:
        """ The content type of the message if set. """
        return self._content_type

    @property
    def duplicate(self) -> bool:
        """ Indicates if the message is a duplicate of an earlier sent item.
        """
        return self._duplicate

    def __str__(self) -> str:
        return str((self.topic, self.payload))


class WillInfo(typing.NamedTuple):
    """ Will information """
    message: AppMessage
    delay_interval: int = 0


# pylint: disable-next=too-many-arguments, too-many-locals, too-many-branches
def make_connect_message(
        version: MQTTVersion,
        client_id: str,
        username: typing.Optional[str],
        password: typing.Optional[typing.Union[str, bytes]],
        clean_start: bool,
        keep_alive: int,
        session_expiry_interval: int,
        receive_max: typing.Optional[int],
        max_packet_size: typing.Optional[int],
        topic_alias_max: int,
        request_response_info: bool,
        request_problem_info: bool,
        user_props: typing.Optional[UserProps],
        auth_method: typing.Optional[str],
        auth_data: typing.Optional[bytes],
        will_info: typing.Optional[WillInfo],
) -> bytes:
    """ Creates a binary MQTT CONNECT message. """
    packer = MessagePacker(PacketType.CONNECT, 0)
    packer.write_string("MQTT")  # protocol name
    version = MQTTVersion(version)
    packer.write_byte(version)  # protocol version

    # write connect flags
    if username is None:
        flags = 0
    else:
        flags = 128
    if password is not None:
        flags += 64
    if will_info is not None:
        if will_info.message.retain:
            flags += 32
        will_qos = Qos(will_info.message.qos)
        flags += will_qos * 8
        flags += 4
    if clean_start:
        flags += 2
    packer.write_byte(flags)

    packer.write_int2(keep_alive)  # keep alive interval

    # properties
    props: typing.List[typing.Tuple[PropertyID, typing.Any]] = [
        (PropertyID.SESSION_EXPIRY_INTERVAL, session_expiry_interval),
        (PropertyID.RECEIVE_MAX, receive_max),
        (PropertyID.MAX_PACKET_SIZE, max_packet_size),
        (PropertyID.TOPIC_ALIAS_MAX, topic_alias_max),
        (PropertyID.REQUEST_RESPONSE_INFO, request_response_info),
        (PropertyID.REQUEST_PROBLEM_INFO, request_problem_info),
        (PropertyID.AUTH_METHOD, auth_method),
        (PropertyID.AUTH_DATA, auth_data),
    ]
    if user_props:
        props.extend([
            (PropertyID.USER_PROPS, user_prop)
            for user_prop in user_props])
    packer.write_props(props)

    # client_id
    packer.write_string(client_id)

    if will_info:
        # will properties
        props = [
            (PropertyID.WILL_DELAY_INTERVAL, will_info.delay_interval),
            (PropertyID.PAYLOAD_FORMAT_INDICATOR,
             will_info.message.payload_format),
            (PropertyID.MESSAGE_EXPIRY_INTERVAL,
             will_info.message.expiry_interval),
            (PropertyID.CONTENT_TYPE, will_info.message.content_type),
            (PropertyID.RESPONSE_TOPIC, will_info.message.response_topic),
            (PropertyID.CORRELATION_DATA, will_info.message.correlation_data),
        ]
        if will_info.message.user_props:
            props.extend([(
                PropertyID.USER_PROPS, user_prop)
                for user_prop in will_info.message.user_props])
        packer.write_props(props)

        packer.write_string(will_info.message.topic)  # will topic

        # will payload
        packer.write_bytes(will_info.message.raw_payload)

    # username
    if username is not None:
        packer.write_string(username)

    # password
    if password is not None:
        if isinstance(password, str):
            password = password.encode()
        packer.write_bytes(password)
    return bytes(packer)


def make_subscribe_message(
        packet_id: int,
        subscription_id: typing.Optional[int],
        sub_reqs: typing.List[SubscriptionRequest],
        user_props: typing.Optional[UserProps]
) -> bytes:
    """ Creates a binary MQTT SUBSCRIBE message. """
    packer = MessagePacker(PacketType.SUBSCRIBE, 2)

    # packet identifier
    packer.write_int2(packet_id)

    # properties
    props: typing.List[typing.Tuple[PropertyID, typing.Any]] = []
    if subscription_id is not None:
        props.append((PropertyID.SUBSCRIPTION_ID, subscription_id))
    if user_props:
        props.extend([
            (PropertyID.USER_PROPS, user_prop)
            for user_prop in user_props])
    packer.write_props(props)

    # payload with topic filter(s) and options
    for sub_req in sub_reqs:
        flags = (
            Qos(sub_req.max_qos) + bool(sub_req.no_local) * 4 +
            bool(sub_req.retain_as_published) * 8 +
            RetainHandling(sub_req.retain_handling) * 16)
        packer.write_string(sub_req.topic_filter)
        packer.write_byte(flags)

    # send subscription request
    return bytes(packer)


# pylint: disable-next=too-many-instance-attributes
class ClientProtocol(BaseProtocol):
    """ Client Asyncio Protocol """

    _outstanding: asyncio.Semaphore

    def __init__(
            self,
            client: 'Client',
            msg_queue: 'asyncio.Queue[AppMessage]',
    ) -> None:
        super().__init__()
        self._client_id = ""
        self._client = client
        self._msg_queue = msg_queue
        self._read_fut = self._loop.create_future()
        self._session_present = False
        self._status = ClientStatus.CLOSED
        self._packet_futs: typing.Dict[int, 'asyncio.Future[typing.Any]'] = {}
        self._packet_id_counter = 1
        self._subs_counter = 0
        self._subscription_id_available = False
        self._supports_wildcards = False
        self._server_topic_aliases: 'OrderedDict[str, int]' = OrderedDict()
        self._server_topic_alias_max = 0
        self._client_topic_aliases: typing.Dict[int, str] = {}
        self._close_fut = self._loop.create_future()
        self._tasks: 'typing.Set[asyncio.Task[typing.Any]]' = set()
        self._keep_alive = 0
        self._keep_alive_handle: typing.Optional[asyncio.TimerHandle] = None
        self._ping_expired_handle: typing.Optional[asyncio.TimerHandle] = None

    def _create_task(
            self,
            coro: typing.Coroutine[typing.Any, typing.Any, typing.Any],
    ) -> None:
        """ Creates a task and keep a reference while running. """
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    @property
    def client_id(self) -> str:
        """ Client identifier. """
        return self._client_id

    @property
    def supports_wildcards(self) -> bool:
        """ Indicates if the server supports wildcards. """
        return self._supports_wildcards

    def connection_made(  # type: ignore[override]
            self, transport: asyncio.Transport) -> None:
        """ Callback for connect event. """
        self._status = ClientStatus.TCP_CONNECTED
        super().connection_made(transport)

    def connection_lost(self, exc: typing.Optional[Exception]) -> None:
        """ Callback for close event. """
        super().connection_lost(exc)
        self._status = ClientStatus.CLOSED
        for fut in self._packet_futs.values():
            if not fut.done():
                fut.set_exception(ValueError("Connection closed"))
        if not self._close_fut.done():
            self._close_fut.set_result(None)
        if self._keep_alive_handle is not None:
            self._keep_alive_handle.cancel()
            self._keep_alive_handle = None
        if self._ping_expired_handle is not None:
            self._ping_expired_handle.cancel()
            self._ping_expired_handle = None
        self._client = None

    # pylint: disable-next=too-many-arguments, too-many-locals
    async def connect(
            self,
            version: MQTTVersion,
            client_id: typing.Optional[str],
            username: typing.Optional[str],
            password: typing.Optional[typing.Union[str, bytes]],
            clean_start: bool,
            keep_alive: int,
            session_expiry_interval: int,
            receive_max: typing.Optional[int],
            max_packet_size: typing.Optional[int],
            topic_alias_max: int,
            request_response_info: bool,
            request_problem_info: bool,
            user_props: typing.Optional[UserProps],
            auth_method: typing.Optional[str],
            auth_data: typing.Optional[bytes],
            will_info: typing.Optional[WillInfo],
    ) -> None:
        """ Connects the client protocol to the server. """
        if self._status is not ClientStatus.TCP_CONNECTED:
            if self._status is ClientStatus.CONNECTED:
                err_msg = "Client is already connected."
            else:
                err_msg = "Client has invalid connection state."
            raise ValueError(err_msg)

        if client_id is None:
            client_id = ""
        else:
            self._client_id = client_id

        connect_msg = make_connect_message(
            version, client_id, username, password, clean_start, keep_alive,
            session_expiry_interval, receive_max, max_packet_size,
            topic_alias_max, request_response_info, request_problem_info,
            user_props, auth_method, auth_data, will_info
        )
        self._keep_alive = keep_alive
        self._read_fut = self._loop.create_future()
        self._status = ClientStatus.CONNECTING
        await self.write(connect_msg)

        # wait for CONNACK
        await self._read_fut

    def _ping_resp_expired(self) -> None:
        self._create_task(self.close())

    def _process_keep_alive(self) -> None:
        elapsed = self._loop.time() - self._last_sent
        if elapsed >= self._keep_alive:
            # Keepalive interval has passed without messages being sent.
            # Send a PINGREQ and schedule the 'ping expired' function, which
            # will be cancelled when a PINGRESP is received from the
            # server before expiration.
            # Note: according to spec, a server should wait one and a half time
            # the keep alive interval, before it closes the connection.
            # Spec also says: "If a Client does not receive a PINGRESP packet
            # within a reasonable amount of time after it has sent a PINGREQ,
            # it SHOULD close the Network Connection to the Server."
            # It doesn't say what a reasonable time is. Use 5 seconds here.
            self._create_task(self.write(b'\xC0\0'))  # send PINGREQ
            self._ping_expired_handle = self._loop.call_later(
                5, self._ping_resp_expired)
            self._keep_alive_handle = None
        else:
            # keepalive interval not passed yet, reschedule
            self._keep_alive_handle = self._loop.call_later(
                self._keep_alive - elapsed, self._process_keep_alive)

    def handle_pingresp_msg(self, unpacker: UnPacker) -> None:
        """ Handle an incoming PINGRESP message. """
        if self._flags != 0:
            raise MalformedPacket("Invalid flags for PINGRESP message.")
        unpacker.check_end()

        if self._ping_expired_handle is not None:
            # Disable the scheduled 'ping expired' function and reschedule the
            # keep alive checker
            self._ping_expired_handle.cancel()
            self._ping_expired_handle = None
            self._keep_alive_handle = self._loop.call_later(
                self._keep_alive, self._process_keep_alive)

    def handle_connack_msg(self, unpacker: UnPacker) -> None:
        """ Handles a CONNACK message from the server. """

        if self._status != ClientStatus.CONNECTING:
            raise ValueError("Invalid message for state.")
        if self._flags != 0:
            raise MalformedPacket(
                ReasonCode.MALFORMED_PACKET,
                "Invalid fixed header flags for CONNACK message.")

        flags = unpacker.read_byte()
        if flags > 1:
            raise MalformedPacket(
                ReasonCode.MALFORMED_PACKET, "Invalid connack flags.")
        self._session_present = bool(flags)
        reason_code = self.verify_reason_code(unpacker.read_byte())
        props = unpacker.read_props(PacketType.CONNACK)
        self._subscription_id_available = props.get(
            PropertyID.SUBSCRIPTION_ID_AVAILABLE, True)
        self._supports_wildcards = props.get(
            PropertyID.SUPPORTS_WILDCARDS, True)
        self._server_topic_alias_max = props.get(PropertyID.TOPIC_ALIAS_MAX, 0)
        client_id = props.get(PropertyID.ASSIGNED_CLIENT_ID)
        if client_id is not None:
            self._client_id = client_id
        self._keep_alive = props.get(
            PropertyID.SERVER_KEEP_ALIVE, self._keep_alive)
        if self._keep_alive:
            self._keep_alive_handle = self._loop.call_at(
                self._last_sent + self._keep_alive,
                self._process_keep_alive)
        unpacker.check_end()
        if reason_code.is_success:
            self._status = ClientStatus.CONNECTED
            server_max_receive = props.get(PropertyID.RECEIVE_MAX, 4)
            self._outstanding = asyncio.Semaphore(server_max_receive)
            if not self._read_fut.done():
                self._read_fut.set_result(None)
        elif not self._read_fut.done():
            reason_string = props.get(PropertyID.REASON_STRING)
            self._read_fut.set_exception(
                get_mqtt_ex(reason_code, reason_string))

    def _get_packet_id(self) -> int:
        """ Gets a new packet id. """
        while True:
            self._packet_id_counter += 1
            if self._packet_id_counter == 65535:
                self._packet_id_counter = 1
            if self._packet_id_counter not in self._packet_futs:
                return self._packet_id_counter
        #
        # while self._packet_id_counter in self._packet_futs:
        #     self._packet_id_counter += 1
        #     if self._packet_id_counter == 65535:
        #         self._packet_id_counter = 1
        # return self._packet_id_counter

    def _get_subscription_id(self) -> int:
        """ Get a new subscription id. """
        if self._subs_counter == 268435455:
            self._subs_counter = 1
        else:
            self._subs_counter += 1
        return self._subs_counter

    async def subscribe(
            self,
            sub_reqs: typing.List[SubscriptionRequest],
            user_props: typing.Optional[UserProps]
    ) -> typing.List[typing.Union[MQTTException, Subscription]]:
        """ Subscribe to a topic filter. """

        packet_id = self._get_packet_id()
        if self._subscription_id_available:
            subscription_id = self._get_subscription_id()
        else:
            subscription_id = None
        sub_msg = make_subscribe_message(
            packet_id, subscription_id, sub_reqs, user_props)

        sub_fut = self._loop.create_future()
        self._packet_futs[packet_id] = sub_fut
        await self.write(sub_msg)
        reason_codes: typing.List[ReasonCode] = (await sub_fut)[0]
        if len(reason_codes) != len(sub_reqs):
            raise ProtocolError(
                ReasonCode.PROTOCOL_ERROR,
                "Wrong number of reason codes in SUBACK message.")
        ret_vals = []
        for reason_code, sub_req in zip(reason_codes, sub_reqs):
            success = reason_code.is_success
            value: typing.Union[Subscription, MQTTException]
            if success:
                value = Subscription(
                    sub_req.topic_filter, Qos(reason_code), subscription_id,
                    sub_req.no_local, sub_req.retain_as_published,
                    sub_req.retain_handling, self._client)
            else:
                value = get_mqtt_ex(reason_code)
            ret_vals.append(value)
        return ret_vals

    async def subscribe_single(
            self,
            sub_req: SubscriptionRequest,
            user_props: typing.Optional[UserProps]) -> Subscription:
        """ Subscribe to a single topic filter. """
        sub = (await self.subscribe([sub_req], user_props))[0]
        if isinstance(sub, MQTTException):
            raise sub
        return sub

    def _handle_suback_msg(
            self, unpacker: UnPacker, packet_type: PacketType) -> None:
        """ Handles a SUBACK or UNSUBACK message from the server. """

        if self._flags != 0:
            raise MalformedPacket(
                ReasonCode.MALFORMED_PACKET, "Invalid flag value for suback.")
        packet_id = unpacker.read_int2()
        props = unpacker.read_props(packet_type)
        reason_codes = []
        while not unpacker.at_end():
            reason_code_val = unpacker.read_byte()
            reason_code = self.verify_reason_code(reason_code_val)
            reason_codes.append(reason_code)

        fut = self._packet_futs.pop(packet_id)
        if not fut.done():
            fut.set_result((
                reason_codes, props.get(PropertyID.REASON_STRING),
                props.get(PropertyID.USER_PROPS)))

    def handle_suback_msg(self, unpacker: UnPacker) -> None:
        """ Handles a SUBACK message from the server. """
        return self._handle_suback_msg(unpacker, PacketType.SUBACK)

    async def unsubscribe(
            self, topics: typing.List[str]
    ) -> typing.List[typing.Union[ReasonCode, MQTTException]]:
        """ Unsubscribe from a list of topic filters. """

        packer = MessagePacker(PacketType.UNSUBSCRIBE, 2)

        # packet identifier
        packet_id = self._get_packet_id()
        packer.write_int2(packet_id)

        packer.write_props([])
        for topic in topics:
            packer.write_string(topic)
        unsub_msg = bytes(packer)
        unsub_fut = self._loop.create_future()
        self._packet_futs[packet_id] = unsub_fut
        await self.write(unsub_msg)
        reason_codes = (await unsub_fut)[0]
        if len(reason_codes) != len(topics):
            raise ProtocolError(
                ReasonCode.PROTOCOL_ERROR,
                "Wrong number of reason codes in SUBACK message.")
        ret_vals: typing.List[typing.Union[ReasonCode, MQTTException]] = []
        for reason_code_val, topic in zip(reason_codes, topics):
            reason_code = verify_reason_code(
                reason_code_val, PacketType.UNSUBACK)
            if reason_code.is_success:
                ret_vals.append(reason_code)
            else:
                ret_vals.append(get_mqtt_ex(reason_code))
        return ret_vals

    async def unsubscribe_single(self, topic: str) -> ReasonCode:
        """ Unsubscribe from a topic filter. """
        result = (await self.unsubscribe([topic]))[0]
        if isinstance(result, MQTTException):
            raise result
        return result

    def handle_unsuback_msg(self, unpacker: UnPacker) -> None:
        """ Handles an UNSUBACK message from the server. """
        self._handle_suback_msg(unpacker, PacketType.UNSUBACK)

    async def publish(
            self, msg: AppMessage
    ) -> typing.Union[None, typing.Tuple[
            ReasonCode, typing.Dict[PropertyID, typing.Any]]]:
        """ Publishes an application message. """

        flags = msg.duplicate * 8 + msg.qos * 2 + msg.retain
        packer = MessagePacker(PacketType.PUBLISH, flags)

        topic = msg.topic
        if self._server_topic_alias_max:
            # topic alias is supported, look up in alias map
            topic_alias = self._server_topic_aliases.get(topic)
            if topic_alias is None:
                # topic not aliased yet
                num_aliases = len(self._server_topic_aliases)
                if num_aliases == self._server_topic_alias_max:
                    # alias map is full, remove oldest and use alias
                    topic_alias = self._server_topic_aliases.popitem(
                        last=False)[1]
                else:
                    # Alias cache not full, add one. Alias can not be 0
                    topic_alias = num_aliases + 1
                self._server_topic_aliases[topic] = topic_alias
            else:
                # topic alias is already known on server
                topic = ""
        else:
            topic_alias = None

        packer.write_string(topic)
        if msg.qos > Qos.AT_MOST_ONCE:
            # packet id will be filled later, write zero for now
            packer.write_int2(0)
        props: typing.List[typing.Tuple[PropertyID, typing.Any]] = [
            (PropertyID.PAYLOAD_FORMAT_INDICATOR, msg.payload_format),
            (PropertyID.MESSAGE_EXPIRY_INTERVAL, msg.expiry_interval),
            (PropertyID.RESPONSE_TOPIC, msg.response_topic),
            (PropertyID.CORRELATION_DATA, msg.correlation_data),
            (PropertyID.CONTENT_TYPE, msg.content_type),
            (PropertyID.TOPIC_ALIAS, topic_alias),
        ]
        if msg.user_props:
            props.extend(
                [(PropertyID.USER_PROPS, val) for val in msg.user_props])
        packer.write_props(props)
        packer.write_raw_bytes(msg.raw_payload)
        if msg.qos is not Qos.AT_MOST_ONCE:
            await self._outstanding.acquire()
            msg_fut: 'asyncio.Future[typing.Tuple[ReasonCode, typing.Dict[PropertyID, typing.Any]]]' = (
                self._loop.create_future())
            packet_id = self._get_packet_id()
            self._packet_futs[packet_id] = msg_fut
            packer.vals[2] = packet_id
        publish_msg = bytes(packer)
        await self.write(publish_msg)
        if msg.qos is Qos.AT_MOST_ONCE:
            return None
        return await msg_fut

    def _release_fut(
            self, packet_id: int) -> typing.Optional['asyncio.Future[typing.Any]']:
        """ Gets a future that represents an outstanding PUBACK or PUBCOMP and
        decrement the number of outstanding operations.
        """
        fut = self._packet_futs.pop(packet_id, None)
        if fut is None:
            return None
        self._outstanding.release()
        return fut

    def _handle_puback_comp_msg(
            self, unpacker: UnPacker, packet_type: PacketType) -> None:
        """ Handles an incoming PUBACK or PUBCOMP message from the server. """
        if self._flags != 0:
            raise MalformedPacket(
                ReasonCode.MALFORMED_PACKET, "Invalid flags.")
        packet_id = unpacker.read_int2()
        if unpacker.at_end():
            reason_code = ReasonCode.SUCCESS
        else:
            reason_code_val = unpacker.read_byte()
            reason_code = self.verify_reason_code(reason_code_val)
        if unpacker.at_end():
            props = {}
        else:
            props = unpacker.read_props(packet_type)
        reason_string = props.get(PropertyID.REASON_STRING)
        # user_props = props.get(PropertyID.USER_PROPS)
        fut = self._release_fut(packet_id)
        if fut is None or fut.done():
            return
        if reason_code.is_success:
            fut.set_result((reason_code, props))
        else:
            fut.set_exception(get_mqtt_ex(reason_code, reason_string))

    def handle_puback_msg(self, unpacker: UnPacker) -> None:
        """ Handles an incoming PUBACK message from the server. """
        return self._handle_puback_comp_msg(unpacker, PacketType.PUBACK)

    def handle_pubrec_msg(self, unpacker: UnPacker) -> None:
        """ Handles an incoming PUBREC message from the server. """

        if self._flags != 0:
            raise MalformedPacket(
                ReasonCode.MALFORMED_PACKET,
                "Invalid flags for PUBREC message.")
        packet_id = unpacker.read_int2()
        if unpacker.at_end():
            reason_code = ReasonCode.SUCCESS
            props: typing.Dict[PropertyID, typing.Any] = {}
        else:
            reason_code_val = unpacker.read_byte()
            reason_code = self.verify_reason_code(reason_code_val)
            if unpacker.at_end():
                props = {}
            else:
                props = unpacker.read_props(PacketType.PUBREC)
        if reason_code.is_success:
            packer = MessagePacker(PacketType.PUBREL, 2)
            packer.write_int2(packet_id)
            if packet_id not in self._packet_futs:
                packer.write_byte(ReasonCode.PACKET_ID_NOT_FOUND)
                packer.write_props([])
            self._create_task(self.write(bytes(packer)))
        else:
            fut = self._release_fut(packet_id)
            if fut is None or fut.done():
                return
            reason_string = props.get(PropertyID.REASON_STRING)
            fut.set_exception(get_mqtt_ex(reason_code, reason_string))

    def handle_pubcomp_msg(self, unpacker: UnPacker) -> None:
        """ Handles an incoming PUBCOMP message from the server. """
        return self._handle_puback_comp_msg(unpacker, PacketType.PUBCOMP)

    def _get_publish_flags(self) -> typing.Tuple[Qos, bool, bool]:
        """ Get the flag values from a PUBLISH message. """
        flags, retain = divmod(self._flags, 2)
        dup_val, qos_val = divmod(flags, 4)
        try:
            qos = Qos(qos_val)
        except ValueError as ex:
            raise MalformedPacket(
                ReasonCode.MALFORMED_PACKET, "Invalid QOS value.") from ex
        duplicate = bool(dup_val)
        if duplicate and qos is Qos.AT_MOST_ONCE:
            raise ProtocolError(
                ReasonCode.PROTOCOL_ERROR,
                "Duplicate flag can not be set for QOS level 0'.")
        return qos, bool(retain), duplicate

    def _handle_topic_alias(
            self,
            props: typing.Dict[PropertyID, typing.Any],
            topic: str,
    ) -> str:
        """ Handles topic alias logic for incoming messages. """
        topic_alias = props.get(PropertyID.TOPIC_ALIAS)
        if topic_alias is not None:
            if topic:
                # Server is setting alias for topic
                self._client_topic_aliases[topic_alias] = topic
            else:
                # Server is using alias instead of topic name
                if topic_alias not in self._client_topic_aliases:
                    raise get_mqtt_ex(
                        ReasonCode.INVALID_TOPIC_ALIAS, "Invalid topic alias.")
                return self._client_topic_aliases[topic_alias]
        return topic

    def _get_publish_msg(
            self, unpacker: UnPacker,
    ) -> typing.Tuple[AppMessage, typing.Optional[int]]:
        """ Gets an Application message from the binary MQTT server message.
        """
        qos, retain, duplicate = self._get_publish_flags()

        topic = unpacker.read_string()
        if qos is not Qos.AT_MOST_ONCE:
            packet_id = unpacker.read_int2()
        else:
            packet_id = None
        publish_props = unpacker.read_props(PacketType.PUBLISH)
        payload = unpacker.read_remaining_bytes()
        topic = self._handle_topic_alias(publish_props, topic)

        return AppMessage(
            topic,
            payload,
            publish_props.get(
                PropertyID.PAYLOAD_FORMAT_INDICATOR,
                PayloadFormat.UNSPECIFIED),
            qos,
            publish_props.get(PropertyID.MESSAGE_EXPIRY_INTERVAL, None),
            publish_props.get(PropertyID.RESPONSE_TOPIC),
            publish_props.get(PropertyID.CORRELATION_DATA),
            publish_props.get(PropertyID.USER_PROPS),
            publish_props.get(PropertyID.SUBSCRIPTION_ID),
            publish_props.get(PropertyID.CONTENT_TYPE),
            retain,
            duplicate
        ), packet_id

    def handle_publish_msg(self, unpacker: UnPacker) -> None:
        """ Handles an incoming PUBLISH message from the server. """

        msg, packet_id = self._get_publish_msg(unpacker)
        self._msg_queue.put_nowait(msg)
        if packet_id is None:
            return
        if msg.qos is Qos.AT_LEAST_ONCE:
            packer = MessagePacker(PacketType.PUBACK, 0)
            packer.write_int2(packet_id)
            bin_msg = bytes(packer)
            self._create_task(self.write(bin_msg))
        else:
            packer = MessagePacker(PacketType.PUBREC, 0)
            packer.write_int2(packet_id)
            bin_msg = bytes(packer)
            self._create_task(self.write(bin_msg))

    def handle_pubrel_msg(self, unpacker: UnPacker) -> None:
        """ Handles an incoming PUBREL message from the server. """

        if self._flags != 2:
            raise get_mqtt_ex(
                ReasonCode.MALFORMED_PACKET,
                "Invalid flags for PUBREL message")
        packet_id = unpacker.read_int2()
        if not unpacker.at_end():
            reason_code_val = unpacker.read_byte()
            self.verify_reason_code(reason_code_val)
            if not unpacker.at_end():
                unpacker.read_props(PacketType.PUBREL)
        unpacker.check_end()

        packer = MessagePacker(PacketType.PUBCOMP, 0)
        packer.write_int2(packet_id)
        self._create_task(self.write(bytes(packer)))

    def handle_disconnect_msg(self, unpacker: UnPacker) -> None:
        """ Handles an incoming DISCONNECT message from the server. """

        if self._flags != 0:
            raise MalformedPacket(
                ReasonCode.MALFORMED_PACKET, "Invalid flags for disconnect.")
        if unpacker.buf_len == 0:
            reason_code = ReasonCode.NORMAL_DISCONNECT
            props: typing.Dict[PropertyID, typing.Any] = {}
        else:
            reason_code_byte = unpacker.read_byte()
            if reason_code_byte == 0:
                reason_code = ReasonCode.NORMAL_DISCONNECT
            else:
                reason_code = ReasonCode(reason_code_byte)
            if unpacker.buf_len == 1:
                props = {}
            else:
                props = unpacker.read_props(PacketType.DISCONNECT)
        unpacker.check_end()

        if self._packet_futs:
            reason_string = props.get(PropertyID.REASON_STRING)
            exc = get_mqtt_ex(reason_code, reason_string)
            for fut in self._packet_futs.values():
                if not fut.done():
                    fut.set_exception(exc)
            self._packet_futs.clear()

        if self._transport is not None:
            self._transport.close()

    async def disconnect(
            self,
            reason_code: ReasonCode = ReasonCode.NORMAL_DISCONNECT) -> None:
        """ Disconnect the client protocol. """
        if self._transport is None or self._transport.is_closing():
            return
        if self._status is ClientStatus.CONNECTED:
            # send disconnect message
            packer = MessagePacker(PacketType.DISCONNECT, 0)
            if reason_code is not ReasonCode.NORMAL_DISCONNECT:
                packer.write_byte(reason_code)
                packer.write_props([])
            self._status = ClientStatus.DISCONNECTING
            try:
                await asyncio.wait_for(self.write(bytes(packer)), 1)
            except asyncio.CancelledError:
                pass
        await self.close()

    async def close(self) -> None:
        """ Close the client protocol. """
        if self._transport is None or self._transport.is_closing():
            return
        self._status = ClientStatus.CLOSING
        try:
            self._transport.close()
            await asyncio.wait_for(self._close_fut, 1)
        except asyncio.CancelledError:
            self._transport.abort()

    def buffer_updated(self, nbytes: int) -> None:
        """ Callback for incoming data. """
        try:
            super().buffer_updated(nbytes)
        except MQTTException as exc:
            if self._transport is None or self._transport.is_closing():
                return
            if self._status is ClientStatus.CONNECTED:
                packer = MessagePacker(PacketType.DISCONNECT, 0)
                packer.write_byte(exc.reason_code)
                packer.write_props([])
                self._transport.write(bytes(packer))
            for fut in self._packet_futs.values():
                if not fut.done():
                    fut.set_exception(exc)
            self._transport.close()


_has_ssl_shutdown_timeout = sys.version_info > (3, 11)


SubscriptionRequestArg = typing.Union[
    str, SubscriptionRequest, typing.Tuple[typing.Any]]


class Client:
    """ Asyncio MQTT client class. """

    # pylint: disable-next=too-many-arguments
    def __init__(
            self,
            host: str,
            port: typing.Optional[int] = None,
            ssl: typing.Union[bool, SSLContext, None] = None,
            family: int = 0,
            local_addr: typing.Optional[typing.Tuple[str, int]] = None,
            server_hostname: typing.Optional[str] = None,
            ssl_handshake_timeout: typing.Optional[float] = None,
            ssl_shutdown_timeout: typing.Optional[float] = None,
    ):
        self._client_id = ""
        self._host = host
        self._port = port if port else (8883 if ssl else 1883)
        self._conn_params = {
            "ssl": ssl,
            "family": family,
            "proto": socket.IPPROTO_TCP,
            "local_addr": local_addr,
            "server_hostname": server_hostname,
            "ssl_handshake_timeout": ssl_handshake_timeout,
        }
        if _has_ssl_shutdown_timeout:
            self._conn_params["ssl_shutdown_timeout"] = ssl_shutdown_timeout
        self._loop = asyncio.get_event_loop()
        self._protocol: typing.Optional[ClientProtocol] = None
        self._msq_queue: 'asyncio.Queue[AppMessage]' = asyncio.Queue()

    # pylint: disable-next=too-many-locals
    async def connect(
            self,
            client_id: typing.Optional[str] = None,
            user_name: typing.Optional[str] = None,
            password: typing.Optional[typing.Union[str, bytes]] = None,
            *,
            clean_start: bool = False,
            keep_alive: int = 0,
            session_expiry_interval: int = 0,
            receive_max: typing.Optional[int] = None,
            max_packet_size: typing.Optional[int] = None,
            topic_alias_max: int = 20,
            request_response_info: bool = False,
            request_problem_info: bool = True,
            user_props: typing.Optional[UserProps] = None,
            auth_method: typing.Optional[str] = None,
            auth_data: typing.Optional[bytes] = None,
            will_info: typing.Optional[WillInfo] = None,
    ) -> None:
        """ Connects to the server. """
        self._protocol = (await self._loop.create_connection(
            self._create_protocol, self._host, self._port,
            **self._conn_params))[1]  # type: ignore[arg-type]
        await self._protocol.connect(
            MQTTVersion.V5, client_id, user_name, password, clean_start,
            keep_alive, session_expiry_interval, receive_max, max_packet_size,
            topic_alias_max, request_response_info, request_problem_info,
            user_props, auth_method, auth_data, will_info)
        self._client_id = self._protocol.client_id

    def _create_protocol(self) -> ClientProtocol:
        return ClientProtocol(self, self._msq_queue)

    @property
    def client_id(self) -> str:
        """ Client identifier. """
        return self._client_id

    async def get_message(self) -> AppMessage:
        msg = await self._msq_queue.get()
        self._msq_queue.task_done()
        return msg

    @typing.overload
    async def subscribe(
            self,
            sub_reqs: SubscriptionRequestArg,
            user_props: typing.Optional[UserProps] = None) -> Subscription:
        ...

    @typing.overload
    async def subscribe(
            self,
            sub_reqs: typing.List[SubscriptionRequestArg],
            user_props: typing.Optional[UserProps] = None,
    ) -> typing.List[typing.Union[Subscription, MQTTException]]:
        ...

    async def subscribe(
            self,
            sub_reqs: typing.Union[
                SubscriptionRequestArg, typing.List[SubscriptionRequestArg]],
            user_props: typing.Optional[UserProps] = None,
    ) -> typing.Union[
            Subscription,
            typing.List[typing.Union[Subscription, MQTTException]]]:
        """ Subscribe to one or more topic filters. """
        if self._protocol is None:
            raise ValueError("Connection is closed")

        if isinstance(sub_reqs, str):
            if not sub_reqs:
                raise ValueError("Empty topic")
            sub_req = SubscriptionRequest(sub_reqs)
        elif isinstance(sub_reqs, tuple):
            sub_req = SubscriptionRequest(*sub_reqs)
        elif isinstance(sub_reqs, SubscriptionRequest):
            sub_req = sub_reqs
        else:
            subs_list = [
                sub_req if isinstance(sub_req, SubscriptionRequest) else
                SubscriptionRequest(sub_req) if isinstance(sub_req, str) else
                SubscriptionRequest(*sub_req) for sub_req in sub_reqs
            ]
            if not subs_list:
                raise ValueError("Subscription requests list is empty.")
            return await self._protocol.subscribe(subs_list, user_props)

        return await self._protocol.subscribe_single(sub_req, user_props)

    @typing.overload
    async def unsubscribe(self, topic: str) -> ReasonCode:
        ...

    @typing.overload
    async def unsubscribe(
            self, topic: typing.List[str]
    ) -> typing.List[typing.Union[ReasonCode, MQTTException]]:
        ...

    async def unsubscribe(
            self, topic: typing.Union[typing.List[str], str]
    ) -> typing.Union[
            ReasonCode, typing.List[typing.Union[ReasonCode, MQTTException]]]:
        """ Unsubscribes from one or more topic filters. """

        if self._protocol is None:
            raise ValueError("Connection is closed.")
        if not topic:
            raise ValueError("Topic name can not be empty.")
        if isinstance(topic, str):
            return await self._protocol.unsubscribe_single(topic)
        if any(not t for t in topic):
            raise ValueError("Topic name can not be empty.")
        return await self._protocol.unsubscribe(topic)

    async def publish(
            self,
            msg: typing.Union[
                AppMessage, typing.Tuple[str, typing.Union[str, bytes]]],
    ) -> typing.Union[None, typing.Tuple[
            ReasonCode, typing.Dict[PropertyID, typing.Any]]]:
        """ Publish an application message. """

        if self._protocol is None:
            raise ValueError("Connection is closed.")
        if not isinstance(msg, AppMessage):
            topic, payload = msg
            msg = AppMessage(topic, payload)
        return await self._protocol.publish(msg)

    async def disconnect(self) -> None:
        """ Disconnects the client. """
        if self._protocol is None:
            return
        protocol = self._protocol
        self._protocol = None
        await protocol.disconnect()
