# SPDX-License-Identifier: BSD-2-Clause

""" Asyncio MQTT client implementation"""
from asyncio import (
    CancelledError, create_task, Future, get_event_loop, Queue, Semaphore,
    TimerHandle, Task, Transport, wait_for)
from codecs import decode
from collections import OrderedDict
import dataclasses
from datetime import timedelta
import enum
from functools import partial
import logging
import socket
from ssl import SSLContext
import sys
from typing import (
    Any, Coroutine, Dict, Iterable, List, Optional, overload, Set, Tuple,
    Union)

from .base_protocol import (
    BaseProtocol, PayloadFormat, Qos, MQTTVersion, PropertyID, PacketType,
    MessagePacker, UnPacker, RetainHandling, ReasonCode, MQTTException,
    get_mqtt_ex, verify_reason_code, default_packet_props,
)
from .utils import LogTextWrapper

_logger = logging.getLogger(__name__)


class ClientStatus(enum.IntEnum):
    """ ClientStatus """
    CLOSED = 0
    TCP_CONNECTED = 1
    CONNECTING = 2
    CONNECTED = 3
    DISCONNECTING = 4
    CLOSING = 5


UserProps = Iterable[Tuple[str, str]]


@dataclasses.dataclass
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


@dataclasses.dataclass
class Subscription:
    """ Subscription """

    topic_filter: str
    qos: Qos
    subscription_id: Optional[int]
    no_local: bool
    retain_as_published: bool
    retain_handling: RetainHandling
    _client: 'Client'

    async def unsubscribe(self) -> ReasonCode:
        """ Unsubscribes from the topic. """
        return await self._client.unsubscribe(self.topic_filter)


if sys.version_info < (3, 10):

    def field(
            *,
            default: Any = dataclasses.MISSING,
            init: bool = True,
            repr: bool = True,  # pylint: disable=redefined-builtin
            compare: bool = True,
            # pylint: disable-next=unused-argument
            kw_only: Union[dataclasses._MISSING_TYPE, bool] =
                dataclasses.MISSING) -> Any:
        """ Proxy function to ignore kw_only argument. """
        return dataclasses.field(
            default=default, init=init, repr=repr, compare=compare)
else:
    field = dataclasses.field


@dataclasses.dataclass
# pylint: disable-next=too-many-instance-attributes
class AppMessage:
    """ An application message. """

    topic: str
    payload: Union[bytes, memoryview, bytearray, str] = field(compare=False)
    qos: Qos = field(repr=False, default=Qos.AT_MOST_ONCE, kw_only=True)
    payload_format: Optional[PayloadFormat] = field(
        default=None, repr=False, kw_only=True)
    expiry_interval: Optional[Union[int, timedelta]] = field(
        default=None, repr=False, kw_only=True)
    response_topic: Optional[str] = field(
        default=None, repr=False, kw_only=True)
    correlation_data: Optional[bytes] = field(
        default=None, repr=False, kw_only=True)
    user_props: Optional[UserProps] = field(
        default=None, repr=False, kw_only=True)
    content_type: Optional[str] = field(
        default=None, repr=False, kw_only=True)
    retain: bool = field(default=False, repr=False, kw_only=True)
    subscription_id: Optional[List[int]] = field(
        default=None, repr=False, init=False)
    duplicate: bool = field(default=False, repr=False, init=False)
    raw_payload: Union[bytes, memoryview, bytearray] = field(
        init=False, repr=False)

    def __post_init__(self) -> None:
        if isinstance(self.payload, str):
            self.raw_payload = self.payload.encode()
            if self.payload_format is None:
                self.payload_format = PayloadFormat.TEXT
        else:
            self.raw_payload = self.payload
            if self.payload_format is PayloadFormat.TEXT:
                self.payload = decode(self.payload)
            else:
                self.payload_format = PayloadFormat.UNSPECIFIED
        if isinstance(self.expiry_interval, timedelta):
            self.expiry_interval = (
                self.expiry_interval.days * 86400 +
                self.expiry_interval.seconds +
                bool(self.expiry_interval.microseconds))
        if self.user_props is not None:
            self.user_props = list(self.user_props)


# pylint: disable-next=too-many-arguments, too-many-locals, too-many-branches
def make_connect_message(
        version: MQTTVersion,
        client_id: str,
        user: Optional[str],
        password: Optional[Union[str, bytes]],
        clean_start: bool,
        keep_alive: int,
        session_expiry_interval: int,
        receive_max: Optional[int],
        max_packet_size: Optional[int],
        topic_alias_max: int,
        request_response_info: bool,
        request_problem_info: bool,
        user_props: Optional[UserProps],
        auth_method: Optional[str],
        auth_data: Optional[bytes],
        will_msg: Optional[AppMessage],
        will_delay_interval: int,
) -> bytes:
    """ Creates a binary MQTT CONNECT message. """
    packer = MessagePacker(PacketType.CONNECT)
    packer.write_string("MQTT")  # protocol name
    version = MQTTVersion(version)
    packer.write_byte(version)  # protocol version

    # write connect flags
    if user is None:
        flags = 0
    else:
        flags = 128
    if password is not None:
        flags += 64
    if will_msg is not None:
        if will_msg.retain:
            flags += 32
        will_qos = Qos(will_msg.qos)
        flags += will_qos * 8
        flags += 4
    if clean_start:
        flags += 2
    packer.write_byte(flags)

    packer.write_int2(keep_alive)  # keep alive interval

    # properties
    props: List[Tuple[PropertyID, Any]] = [
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

    if will_msg:
        # write will message
        props = [
            (PropertyID.WILL_DELAY_INTERVAL, will_delay_interval),
            (PropertyID.PAYLOAD_FORMAT_INDICATOR,
             will_msg.payload_format),
            (PropertyID.MESSAGE_EXPIRY_INTERVAL,
             will_msg.expiry_interval),
            (PropertyID.CONTENT_TYPE, will_msg.content_type),
            (PropertyID.RESPONSE_TOPIC, will_msg.response_topic),
            (PropertyID.CORRELATION_DATA, will_msg.correlation_data),
        ]
        if will_msg.user_props:
            props.extend([(
                PropertyID.USER_PROPS, user_prop)
                for user_prop in will_msg.user_props])
        packer.write_props(props)
        packer.write_string(will_msg.topic)
        packer.write_bytes(will_msg.raw_payload)

    # user
    if user is not None:
        packer.write_string(user)

    # password
    if password is not None:
        if isinstance(password, str):
            password = password.encode()
        packer.write_bytes(password)
    return bytes(packer)


def make_subscribe_message(
        packet_id: int,
        subscription_id: Optional[int],
        sub_reqs: List[SubscriptionRequest],
        user_props: Optional[UserProps]
) -> bytes:
    """ Creates a binary MQTT SUBSCRIBE message. """
    packer = MessagePacker(PacketType.SUBSCRIBE)

    # packet identifier
    packer.write_int2(packet_id)

    # properties
    props: List[Tuple[PropertyID, Any]] = []
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


MessageProps = Dict[PropertyID, Any]


def _fut_set_result(fut: Optional['Future[Any]'], result: Any) -> None:
    if fut is None or fut.done():
        return
    fut.set_result(result)


def _fut_set_ex(fut: Optional['Future[Any]'], exc: Exception) -> None:
    if fut is None or fut.done():
        return
    fut.set_exception(exc)


def _fut_set_ex_from_code_and_props(
        fut: Optional['Future[Any]'],
        reason_code: ReasonCode,
        props: Dict[PropertyID, Any],
) -> None:
    _fut_set_ex(fut, get_mqtt_ex(reason_code, props[PropertyID.REASON_STRING]))


# pylint: disable-next=too-many-instance-attributes
class ClientProtocol(BaseProtocol):
    """ Client Asyncio Protocol """

    _outstanding: Semaphore

    def __init__(
            self,
            client: 'Client',
            msg_queue: 'Queue[AppMessage]',
    ) -> None:
        super().__init__()
        self.client_id = ""
        self._client = client
        self._msg_queue = msg_queue
        self._conn_fut: Optional['Future[None]'] = None
        self._session_present = False
        self._status = ClientStatus.CLOSED
        self._packet_futs: Dict[int, 'Future[Any]'] = {}
        self._packet_id_counter = 0
        self._subs_counter = 0
        self._subscription_id_available = False
        self.supports_wildcards = False
        self._server_topic_aliases: 'OrderedDict[str, int]' = OrderedDict()
        self._server_topic_alias_max = 0
        self._client_topic_aliases: Dict[int, str] = {}
        self._client_topic_alias_max = 0
        self._close_fut = self._loop.create_future()
        self._tasks: 'Set[Task[Any]]' = set()
        self._keep_alive_interval = 0
        self._keep_alive_handle: Optional[TimerHandle] = None
        self._ping_expired_handle: Optional[TimerHandle] = None
        self._response_timeout: Optional[float] = None

    def _create_task(self, coro: Coroutine[Any, Any, Any],) -> None:
        """ Creates a task and keep a reference while running. """
        task = create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def connection_made(  # type: ignore[override]
            self, transport: Transport) -> None:
        """ Callback for connect event. """
        self._status = ClientStatus.TCP_CONNECTED
        super().connection_made(transport)

    def _close_futures(self, exc: Optional[Exception]) -> None:
        if exc is None:
            exc = ValueError("Connection is closed.")
        _fut_set_ex(self._conn_fut, exc)
        self._conn_fut = None
        for fut in self._packet_futs.values():
            _fut_set_ex(fut, exc)
        _fut_set_result(self._close_fut, None)
        if self._keep_alive_handle is not None:
            self._keep_alive_handle.cancel()
            self._keep_alive_handle = None
        if self._ping_expired_handle is not None:
            self._ping_expired_handle.cancel()
            self._ping_expired_handle = None

    def connection_lost(self, exc: Optional[Exception]) -> None:
        """ Callback for close event. """
        super().connection_lost(exc)
        self._status = ClientStatus.CLOSED
        self._close_futures(exc)

    def write_nowait(self, data: bytes) -> None:
        if self._writing_enabled.is_set():
            super().write_nowait(data)
        else:
            self._create_task(self.write(data))

    # pylint: disable-next=too-many-arguments, too-many-locals
    async def connect(
            self,
            version: MQTTVersion,
            client_id: str,
            user: Optional[str],
            password: Optional[Union[str, bytes]],
            response_timeout: Optional[float],
            clean_start: bool,
            keep_alive: int,
            session_expiry_interval: int,
            receive_max: int,
            max_packet_size: Optional[int],
            topic_alias_max: int,
            request_response_info: bool,
            request_problem_info: bool,
            user_props: Optional[UserProps],
            auth_method: Optional[str],
            auth_data: Optional[bytes],
            will_msg: Optional[AppMessage],
            will_delay_interval: int,
    ) -> None:
        """ Connects the client protocol to the server. """

        self.client_id = client_id

        connect_msg = make_connect_message(
            version, client_id, user, password, clean_start, keep_alive,
            session_expiry_interval, receive_max, max_packet_size,
            topic_alias_max, request_response_info, request_problem_info,
            user_props, auth_method, auth_data, will_msg, will_delay_interval,
        )
        self._client_topic_alias_max = topic_alias_max
        self._keep_alive_interval = keep_alive
        self._response_timeout = response_timeout
        self._conn_fut = self._loop.create_future()
        self._status = ClientStatus.CONNECTING
        _logger.debug(
            "> CONNECT: client_id=%s, user=%s", client_id, user or "")
        await self.write(connect_msg)

        # wait for CONNACK
        await wait_for(self._conn_fut, self._response_timeout)

    def _ping_resp_expired(self) -> None:
        self._create_task(self.close())

    def _process_keep_alive(self) -> None:
        elapsed = self._loop.time() - self._last_sent
        if elapsed >= self._keep_alive_interval:
            # Keepalive interval has passed without messages being sent.
            # Send a PINGREQ and schedule the 'ping expired' function, which
            # will be cancelled when a PINGRESP is received from the
            # server before expiration.
            # Note: according to spec, a server should wait one and a half-time
            # the keep alive interval, before it closes the connection.
            # Spec also says: "If a Client does not receive a PINGRESP packet
            # within a reasonable amount of time after it has sent a PINGREQ,
            # it SHOULD close the Network Connection to the Server."
            # It doesn't say what a reasonable time is. Use 5 seconds here.
            _logger.debug("> PINGREQ")
            pingreq_msq = bytes(MessagePacker(PacketType.PINGREQ))
            self.write_nowait(pingreq_msq)
            self._ping_expired_handle = self._loop.call_later(
                5, self._ping_resp_expired)
            self._keep_alive_handle = None
        else:
            # keepalive interval not passed yet, reschedule
            self._keep_alive_handle = self._loop.call_later(
                self._keep_alive_interval - elapsed, self._process_keep_alive)

    def handle_pingresp_msg(self, unpacker: UnPacker) -> None:
        """ Handle an incoming PINGRESP message. """
        _logger.debug("< PINGRESP")
        unpacker.check_end()

        if self._ping_expired_handle is not None:
            # Disable the scheduled 'ping expired' function and reschedule the
            # keep alive checker
            self._ping_expired_handle.cancel()
            self._ping_expired_handle = None
            self._keep_alive_handle = self._loop.call_later(
                self._keep_alive_interval, self._process_keep_alive)

    def handle_connack_msg(self, unpacker: UnPacker) -> None:
        """ Handles a CONNACK message from the server. """

        if self._conn_fut is None:
            raise get_mqtt_ex(
                ReasonCode.PROTOCOL_ERROR, "Unexpected CONNACK message.")

        flags = unpacker.read_byte()
        if flags > 1:
            raise get_mqtt_ex(
                ReasonCode.MALFORMED_PACKET, "Invalid connack flags.")
        self._session_present = bool(flags)
        reason_code = self.verify_reason_code(unpacker.read_byte())
        props = unpacker.read_props(PacketType.CONNACK)

        unpacker.check_end()

        self._subscription_id_available = props[
            PropertyID.SUBSCRIPTION_ID_AVAILABLE]
        self.supports_wildcards = props[PropertyID.SUPPORTS_WILDCARDS]
        self._server_topic_alias_max = props[PropertyID.TOPIC_ALIAS_MAX]
        assigned_client_id = props[PropertyID.ASSIGNED_CLIENT_ID]
        if assigned_client_id is not None:
            self.client_id = assigned_client_id

        _logger.debug(
            "< CONNACK: client_id=%s reason_code=%s", self.client_id,
            reason_code.name)

        server_keep_alive_interval = props[PropertyID.SERVER_KEEP_ALIVE]
        if server_keep_alive_interval is not None:
            self._keep_alive_interval = server_keep_alive_interval
        if self._keep_alive_interval:
            self._keep_alive_handle = self._loop.call_at(
                self._last_sent + self._keep_alive_interval,
                self._process_keep_alive)

        if reason_code.is_success:
            self._status = ClientStatus.CONNECTED
            server_max_receive = props[PropertyID.RECEIVE_MAX]
            if server_max_receive == 0:
                raise get_mqtt_ex(
                    ReasonCode.MALFORMED_PACKET,
                    "Receive Maximum can not be 0")
            self._outstanding = Semaphore(server_max_receive)
            _fut_set_result(self._conn_fut, None)
        else:
            _fut_set_ex_from_code_and_props(self._conn_fut, reason_code, props)
            if self._transport is not None:
                self._transport.close()
        self._conn_fut = None

    def _get_packet_id(self) -> int:
        """ Gets a new packet id. """
        while True:
            if self._packet_id_counter == 65535:
                self._packet_id_counter = 1
            else:
                self._packet_id_counter += 1
            if self._packet_id_counter not in self._packet_futs:
                return self._packet_id_counter

    def _get_subscription_id(self) -> int:
        """ Get a new subscription id. """
        if self._subs_counter == 268435455:
            self._subs_counter = 1
        else:
            self._subs_counter += 1
        return self._subs_counter

    async def subscribe(
            self,
            sub_reqs: List[SubscriptionRequest],
            user_props: Optional[UserProps]
    ) -> List[Union[MQTTException, Subscription]]:
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
        sub_fut.add_done_callback(partial(self._remove_packet_fut, packet_id))
        _logger.debug("> SUBSCRIBE: subs=%s packet_id=%s", sub_reqs, packet_id)
        self.write_nowait(sub_msg)
        reason_codes: List[ReasonCode] = (await sub_fut)[0]
        if len(reason_codes) != len(sub_reqs):
            raise get_mqtt_ex(
                ReasonCode.PROTOCOL_ERROR,
                "Wrong number of reason codes in SUBACK message.")
        ret_vals = []
        for reason_code, sub_req in zip(reason_codes, sub_reqs):
            success = reason_code.is_success
            value: Union[Subscription, MQTTException]
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
            user_props: Optional[UserProps]) -> Subscription:
        """ Subscribe to a single topic filter. """
        sub = (await self.subscribe([sub_req], user_props))[0]
        if isinstance(sub, MQTTException):
            raise sub
        return sub

    def _handle_suback_msg(
            self, unpacker: UnPacker, packet_type: PacketType) -> None:
        """ Handles a SUBACK or UNSUBACK message from the server. """

        packet_id = unpacker.read_int2()
        props = unpacker.read_props(packet_type)
        reason_codes = []
        while not unpacker.at_end():
            reason_code_val = unpacker.read_byte()
            reason_code = self.verify_reason_code(reason_code_val)
            reason_codes.append(reason_code)
        _logger.debug(
            "< %s: reason_codes=%s packet_id=%s", packet_type.name,
            reason_codes, packet_id)
        fut = self._packet_futs.get(packet_id)
        _fut_set_result(fut, (
            reason_codes, props[PropertyID.REASON_STRING],
            props[PropertyID.USER_PROPS]))

    def handle_suback_msg(self, unpacker: UnPacker) -> None:
        """ Handles a SUBACK message from the server. """
        return self._handle_suback_msg(unpacker, PacketType.SUBACK)

    async def unsubscribe(
            self, topics: List[str]
    ) -> List[Union[ReasonCode, MQTTException]]:
        """ Unsubscribe from a list of topic filters. """

        packer = MessagePacker(PacketType.UNSUBSCRIBE)

        # packet identifier
        packet_id = self._get_packet_id()
        packer.write_int2(packet_id)

        packer.write_props([])
        for topic in topics:
            packer.write_string(topic)
        unsub_msg = bytes(packer)
        unsub_fut = self._loop.create_future()
        self._packet_futs[packet_id] = unsub_fut
        unsub_fut.add_done_callback(
            partial(self._remove_packet_fut, packet_id))
        _logger.debug("> UNSUBSCRIBE: subs=%s packet_id=%s", topics, packet_id)
        self.write_nowait(unsub_msg)
        reason_codes = (await unsub_fut)[0]
        if len(reason_codes) != len(topics):
            raise get_mqtt_ex(
                ReasonCode.PROTOCOL_ERROR,
                "Wrong number of reason codes in SUBACK message.")
        ret_vals: List[Union[ReasonCode, MQTTException]] = []
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

    def _create_publish_packet(
            self, msg: AppMessage) -> Tuple[bytes, Optional[int]]:

        flags = msg.duplicate * 8 + msg.qos * 2 + msg.retain
        packer = MessagePacker(PacketType.PUBLISH, flags)

        topic = msg.topic
        if self._server_topic_alias_max:
            # Topic alias is supported by server, use it.
            # Look up in alias map
            topic_alias = self._server_topic_aliases.get(topic)
            if topic_alias is None:
                # Topic not aliased yet
                num_aliases = len(self._server_topic_aliases)
                if num_aliases < self._server_topic_alias_max:
                    # Alias cache not full, add one. Alias can not be 0
                    topic_alias = num_aliases + 1
                else:
                    # alias map is full, remove oldest and use alias
                    topic_alias = self._server_topic_aliases.popitem(
                        last=False)[1]
                self._server_topic_aliases[topic] = topic_alias
            else:
                # Topic alias is already known on server. Move to most recent.
                self._server_topic_aliases.move_to_end(topic)
                topic = ""
        else:
            topic_alias = None

        packer.write_string(topic)
        if msg.qos > Qos.AT_MOST_ONCE:
            packet_id = self._get_packet_id()
            packer.write_int2(packet_id)
        else:
            packet_id = None
        props: List[Tuple[PropertyID, Any]] = [
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
        return bytes(packer), packet_id

    async def publish(
            self, msg: AppMessage
    ) -> Union[None, Tuple[ReasonCode, MessageProps]]:
        """ Publishes an application message. """

        bin_msg, packet_id = self._create_publish_packet(msg)

        msg_fut: Optional['Future[Tuple[ReasonCode, MessageProps]]']
        if packet_id is not None:
            msg_fut = self._loop.create_future()
            self._packet_futs[packet_id] = msg_fut
            msg_fut.add_done_callback(partial(self._release_fut, packet_id))
            await self._outstanding.acquire()
        else:
            msg_fut = None
        _logger.debug(
            "> PUBLISH: topic=%s payload=%s packet_id=%s", msg.topic,
            LogTextWrapper(msg.raw_payload), packet_id)
        self.write_nowait(bin_msg)
        if msg_fut is None:
            return None
        if self._response_timeout is None:
            return await msg_fut
        return await wait_for(msg_fut, self._response_timeout * msg.qos)

    # pylint: disable-next=unused-argument
    def _remove_packet_fut(self, packet_id: int, fut: 'Future[Any]') -> None:
        del self._packet_futs[packet_id]

    def _release_fut(
            # pylint: disable-next=unused-argument
            self, packet_id: int, fut: 'Future[Any]',
    ) -> None:
        """ Releases a future that represents an outstanding PUBACK or PUBCOMP
        """
        del self._packet_futs[packet_id]
        self._outstanding.release()

    def _handle_puback_comp_msg(
            self, unpacker: UnPacker, packet_type: PacketType) -> None:
        """ Handles an incoming PUBACK or PUBCOMP message from the server. """
        packet_id = unpacker.read_int2()
        if unpacker.at_end():
            reason_code = ReasonCode.SUCCESS
        else:
            reason_code_val = unpacker.read_byte()
            reason_code = self.verify_reason_code(reason_code_val)
        if unpacker.at_end():
            props = default_packet_props[packet_type]
        else:
            props = unpacker.read_props(packet_type)
        _logger.debug(
            "< %s: reason_code=%s packet_id=%s", packet_type.name,
            reason_code.name, packet_id)
        fut = self._packet_futs.get(packet_id)
        if reason_code.is_success:
            _fut_set_result(fut, (reason_code, props))
        else:
            _fut_set_ex_from_code_and_props(fut, reason_code, props)

    def handle_puback_msg(self, unpacker: UnPacker) -> None:
        """ Handles an incoming PUBACK message from the server. """
        return self._handle_puback_comp_msg(unpacker, PacketType.PUBACK)

    def handle_pubrec_msg(self, unpacker: UnPacker) -> None:
        """ Handles an incoming PUBREC message from the server. """

        packet_id = unpacker.read_int2()
        if unpacker.at_end():
            reason_code = ReasonCode.SUCCESS
        else:
            reason_code_val = unpacker.read_byte()
            reason_code = self.verify_reason_code(reason_code_val)
        if unpacker.at_end():
            props = default_packet_props[PacketType.PUBREC]
        else:
            props = unpacker.read_props(PacketType.PUBREC)

        _logger.debug(
            "< PUBREC: reason_code=%s packet_id=%s", reason_code.name,
            packet_id)

        if reason_code.is_success:
            packer = MessagePacker(PacketType.PUBREL)
            packer.write_int2(packet_id)
            if packet_id not in self._packet_futs:
                reason_code = ReasonCode.PACKET_ID_NOT_FOUND
                packer.write_byte(reason_code)
                packer.write_props([])
            else:
                reason_code = ReasonCode.SUCCESS
            pubrel_msg = bytes(packer)
            _logger.debug(
                "> PUBREL: reason_code=%s packet_id=%s", reason_code.name,
                packet_id)
            self.write_nowait(pubrel_msg)
        else:
            # An error occurred, a PUBCOMP message will not be sent by the
            # server. Release and set the future now.
            fut = self._packet_futs.get(packet_id)
            _fut_set_ex_from_code_and_props(fut, reason_code, props)

    def handle_pubcomp_msg(self, unpacker: UnPacker) -> None:
        """ Handles an incoming PUBCOMP message from the server. """
        return self._handle_puback_comp_msg(unpacker, PacketType.PUBCOMP)

    def _get_publish_flags(self) -> Tuple[Qos, bool, bool]:
        """ Get the flag values from a PUBLISH message. """
        flags, retain = divmod(self._flags, 2)
        dup_val, qos_val = divmod(flags, 4)
        try:
            qos = Qos(qos_val)
        except ValueError as ex:
            raise get_mqtt_ex(
                ReasonCode.MALFORMED_PACKET, "Invalid QOS value.") from ex
        duplicate = bool(dup_val)
        if duplicate and qos is Qos.AT_MOST_ONCE:
            raise get_mqtt_ex(
                ReasonCode.MALFORMED_PACKET,
                "Duplicate flag can not be set for QOS level 0'.")
        return qos, bool(retain), duplicate

    def _handle_topic_alias(self, props: MessageProps, topic: str) -> str:
        """ Handles topic alias logic for incoming messages.

        Returns the effective topic name.

        """
        topic_alias = props[PropertyID.TOPIC_ALIAS]
        if topic_alias is None:
            # Server has not set a topic alias
            return topic

        if topic_alias == 0:
            raise get_mqtt_ex(
                ReasonCode.INVALID_TOPIC_ALIAS,
                "Topic alias can not be zero.")

        if topic_alias > self._client_topic_alias_max:
            raise get_mqtt_ex(
                ReasonCode.INVALID_TOPIC_ALIAS,
                f"Topic alias '{topic_alias}' is greater than "
                "maximum topic alias "
                f"'{self._client_topic_alias_max}'.")

        if topic:
            # Topic is not empty. Server is setting alias for topic.
            self._client_topic_aliases[topic_alias] = topic
            return topic

        # Topic is empty. Server is using alias instead of topic name
        try:
            return self._client_topic_aliases[topic_alias]
        except KeyError as ex:
            raise get_mqtt_ex(
                ReasonCode.PROTOCOL_ERROR,
                f"Unknown topic alias: {topic_alias}.") from ex

    def _parse_publish_packet(
            self, unpacker: UnPacker,
    ) -> Tuple[AppMessage, Optional[int]]:
        """ Gets an Application message from the binary MQTT server message.
        """
        qos, retain, duplicate = self._get_publish_flags()

        topic = unpacker.read_string()
        if qos is not Qos.AT_MOST_ONCE:
            packet_id = unpacker.read_int2()
        else:
            packet_id = None
        publish_props = unpacker.read_props(PacketType.PUBLISH)
        bin_payload = unpacker.read_remaining_bytes()
        payload_format = publish_props[PropertyID.PAYLOAD_FORMAT_INDICATOR]
        if payload_format is PayloadFormat.TEXT:
            try:
                payload: Union[str, bytes] = bin_payload.decode()
            except UnicodeError as ex:
                # pylint: disable-next=fixme
                # TODO: send PUBREC, PUBACK with error code instead of raising
                # and causing a DISCONNECT when qos permits.
                raise get_mqtt_ex(ReasonCode.INVALID_PAYLOAD_FORMAT) from ex
        else:
            payload = bin_payload

        topic = self._handle_topic_alias(publish_props, topic)

        app_message = AppMessage(
            topic,
            payload,
            qos=qos,
            payload_format=payload_format,
            expiry_interval=publish_props[PropertyID.MESSAGE_EXPIRY_INTERVAL],
            response_topic=publish_props[PropertyID.RESPONSE_TOPIC],
            correlation_data=publish_props[PropertyID.CORRELATION_DATA],
            user_props=publish_props[PropertyID.USER_PROPS],
            content_type=publish_props[PropertyID.CONTENT_TYPE],
            retain=retain,
        )
        app_message.subscription_id = publish_props[PropertyID.SUBSCRIPTION_ID]
        app_message.duplicate = duplicate
        return app_message, packet_id

    def handle_publish_msg(self, unpacker: UnPacker) -> None:
        """ Handles an incoming PUBLISH message from the server. """

        msg, packet_id = self._parse_publish_packet(unpacker)
        _logger.debug(
            "< PUBLISH: topic=%s payload=%s packet_id=%s", msg.topic,
            LogTextWrapper(msg.payload), packet_id)
        self._msg_queue.put_nowait(msg)
        if packet_id is None:
            return
        if msg.qos is Qos.AT_LEAST_ONCE:
            packet_type = PacketType.PUBACK
        else:
            packet_type = PacketType.PUBREC

        packer = MessagePacker(packet_type)
        packer.write_int2(packet_id)
        bin_msg = bytes(packer)
        _logger.debug(
            "> %s: reason_code=SUCCESS packet_id=%s", packet_type.name,
            packet_id)
        self.write_nowait(bin_msg)

    def handle_pubrel_msg(self, unpacker: UnPacker) -> None:
        """ Handles an incoming PUBREL message from the server. """

        packet_id = unpacker.read_int2()
        if unpacker.at_end():
            reason_code = ReasonCode.SUCCESS
        else:
            reason_code_val = unpacker.read_byte()
            reason_code = self.verify_reason_code(reason_code_val)
            if not unpacker.at_end():
                unpacker.read_props(PacketType.PUBREL)

        unpacker.check_end()
        _logger.debug(
            "< PUBREL: reason_code=%s packet_id=%s", reason_code.name,
            packet_id)

        packer = MessagePacker(PacketType.PUBCOMP)
        packer.write_int2(packet_id)
        pubcomp_msg = bytes(packer)
        _logger.debug("> PUBCOMP: reason_code=SUCCESS packet_id=%s", packet_id)
        self.write_nowait(pubcomp_msg)

    def handle_disconnect_msg(self, unpacker: UnPacker) -> None:
        """ Handles an incoming DISCONNECT message from the server. """
        if unpacker.buf_len == 0:
            reason_code = ReasonCode.NORMAL_DISCONNECT
            props = default_packet_props[PacketType.DISCONNECT]
        else:
            reason_code_byte = unpacker.read_byte()
            if reason_code_byte == 0:
                reason_code = ReasonCode.NORMAL_DISCONNECT
            else:
                reason_code = ReasonCode(reason_code_byte)
            if unpacker.buf_len == 1:
                props = default_packet_props[PacketType.DISCONNECT]
            else:
                props = unpacker.read_props(PacketType.DISCONNECT)
        unpacker.check_end()
        _logger.debug("< DISCONNECT: reason_code=%s", reason_code.name)

        if reason_code.is_success:
            exc = None
        else:
            exc = get_mqtt_ex(reason_code, props[PropertyID.REASON_STRING])
        self._close_futures(exc)

        if self._transport is not None:
            self._transport.abort()

    async def disconnect(
            self,
            reason_code: ReasonCode = ReasonCode.NORMAL_DISCONNECT) -> None:
        """ Disconnect the client protocol. """
        if self._transport is None or self._transport.is_closing():
            return
        if self._status is ClientStatus.CONNECTED:
            # send disconnect message
            packer = MessagePacker(PacketType.DISCONNECT)
            if reason_code is not ReasonCode.NORMAL_DISCONNECT:
                packer.write_byte(reason_code)
                packer.write_props([])
            self._status = ClientStatus.DISCONNECTING
            disconnect_msg = bytes(packer)
            reason_code_name = (
                "NORMAL_DISCONNECT"
                if reason_code is ReasonCode.NORMAL_DISCONNECT
                else reason_code.name)
            _logger.debug("> DISCONNECT: reason_code=%s", reason_code_name)
            try:
                await wait_for(self.write(disconnect_msg), 1)
            except CancelledError:
                pass
        await self.close()

    async def close(self) -> None:
        """ Close the client protocol. """
        if self._transport is None or self._transport.is_closing():
            return
        self._status = ClientStatus.CLOSING
        try:
            self._transport.close()
            await wait_for(self._close_fut, 1)
        except CancelledError:
            self._transport.abort()

    def buffer_updated(self, nbytes: int) -> None:
        """ Callback for incoming data. """
        try:
            super().buffer_updated(nbytes)
        except MQTTException as exc:
            if self._transport is None or self._transport.is_closing():
                return
            if self._status is ClientStatus.CONNECTED:
                packer = MessagePacker(PacketType.DISCONNECT)
                packer.write_byte(exc.reason_code)
                packer.write_props([])
                self._transport.write(bytes(packer))
            self._close_futures(exc)
            self._transport.close()


_has_ssl_shutdown_timeout = sys.version_info > (3, 11)

SubscriptionRequestArg = Union[str, SubscriptionRequest, Tuple[Any]]


# pylint: disable-next=too-many-instance-attributes
class Client:
    """ Asyncio MQTT client class. """

    # pylint: disable-next=too-many-arguments,too-many-locals
    def __init__(
            self,

            # Asyncio server base params
            host: str,
            port: Optional[int] = None,

            # MQTT base params
            client_id: str = "",
            user: Optional[str] = None,
            password: Optional[Union[str, bytes]] = None,

            *,

            # Asyncio server params
            ssl: Union[bool, SSLContext, None] = None,
            family: int = 0,
            local_addr: Optional[Tuple[str, int]] = None,
            server_hostname: Optional[str] = None,
            ssl_handshake_timeout: Optional[float] = None,
            ssl_shutdown_timeout: Optional[float] = None,

            # MQTT params
            clean_start: bool = False,
            keep_alive: int = 0,
            response_timeout: Optional[float] = None,
            session_expiry_interval: int = 0,
            receive_max: int = 65535,
            max_packet_size: Optional[int] = None,
            topic_alias_max: int = 20,
            request_response_info: bool = False,
            request_problem_info: bool = True,
            user_props: Optional[UserProps] = None,
            auth_method: Optional[str] = None,
            auth_data: Optional[bytes] = None,
            will_msg: Optional[AppMessage] = None,
            will_delay_interval: int = 0,
    ):
        self._client_id = client_id
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
        if response_timeout is not None:
            response_timeout = float(response_timeout)
            if response_timeout <= 0:
                raise ValueError("Invalid response timeout.")
        self._mqtt_params = (
            MQTTVersion.V5, client_id, user, password, response_timeout,
            clean_start, keep_alive, session_expiry_interval, receive_max,
            max_packet_size, topic_alias_max, request_response_info,
            request_problem_info, user_props, auth_method, auth_data,
            will_msg, will_delay_interval)
        self._loop = get_event_loop()
        self._protocol: Optional[ClientProtocol] = None
        self._msq_queue: 'Queue[AppMessage]' = Queue()

    async def connect(self) -> None:
        """ Connects to the server. """
        if self._protocol is not None:
            raise ValueError("Client is already connected.")
        protocol = (await self._loop.create_connection(
            self._create_protocol, self._host, self._port,
            **self._conn_params))[1]  # type: ignore
        await protocol.connect(*self._mqtt_params)
        self._protocol = protocol
        self._client_id = protocol.client_id

    def _create_protocol(self) -> ClientProtocol:
        return ClientProtocol(self, self._msq_queue)

    @property
    def client_id(self) -> str:
        """ Client identifier. """
        return self._client_id

    async def get_message(self) -> AppMessage:
        """ Get a received application message. """
        msg = await self._msq_queue.get()
        self._msq_queue.task_done()
        return msg

    @overload
    async def subscribe(
            self,
            sub_reqs: SubscriptionRequestArg,
            user_props: Optional[UserProps] = None) -> Subscription:
        """ Subscribe to a topic filter. """

    @overload
    async def subscribe(
            self,
            sub_reqs: List[SubscriptionRequestArg],
            user_props: Optional[UserProps] = None,
    ) -> List[Union[Subscription, MQTTException]]:
        """ Subscribe to a list of topic filters. """

    async def subscribe(
            self,
            sub_reqs: Union[
                SubscriptionRequestArg, List[SubscriptionRequestArg]],
            user_props: Optional[UserProps] = None,
    ) -> Union[
            Subscription,
            List[Union[Subscription, MQTTException]]]:
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

    @overload
    async def unsubscribe(self, topic: str) -> ReasonCode:
        """ Unsubscribes from one topic filter. """

    @overload
    async def unsubscribe(
            self, topic: List[str]
    ) -> List[Union[ReasonCode, MQTTException]]:
        """ Unsubscribes from a list of topic filters. """

    async def unsubscribe(
            self, topic: Union[List[str], str]
    ) -> Union[
            ReasonCode, List[Union[ReasonCode, MQTTException]]]:
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
            msg: Union[
                AppMessage, Tuple[str, Union[str, bytes]]],
    ) -> Union[None, Tuple[ReasonCode, MessageProps]]:
        """ Publish an application message. """

        if self._protocol is None:
            raise ValueError("Connection is closed.")
        if not isinstance(msg, AppMessage):
            topic, payload = msg
            msg = AppMessage(topic, payload)
        return await self._protocol.publish(msg)

    async def close(self) -> None:
        """ Disconnects the client. """
        if self._protocol is None:
            return
        protocol = self._protocol
        self._protocol = None
        await protocol.disconnect()
