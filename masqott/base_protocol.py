# SPDX-License-Identifier: BSD-2-Clause

""" Base functionality for asyncio MQTT protocol. """

import asyncio
from asyncio import BufferedProtocol, Transport
from codecs import decode
import enum
from functools import lru_cache
import struct
from typing import (
    Any, Callable, Dict, Iterable, List, NamedTuple, Optional, Set, Tuple,
    Type, Union)


# ==== Protocol enums =========================================================


class ReasonCode(enum.IntEnum):
    """ MQTT Reason code """
    SUCCESS = 0
    NORMAL_DISCONNECT = 0
    GRANTED_QOS_0 = 0
    GRANTED_QOS_1 = 1
    GRANTED_QOS_2 = 2
    DISCONNECT_WITH_WILL_MSG = 4
    NO_MATCHING_SUBSCRIBERS = 16
    NO_SUBSCRIPTION_EXISTED = 17
    CONTINUE_AUTH = 24
    REAUTHENTICATE = 25
    UNSPECIFIED_ERROR = 128
    MALFORMED_PACKET = 129
    PROTOCOL_ERROR = 130
    IMPLEMENTATION_ERROR = 131
    UNSUPPORTED_PROTOCOL_VERSION = 132
    INVALID_CLIENT_ID = 133
    BAD_USER_OR_PWD = 134
    NOT_AUTHORIZED = 135
    SERVER_UNAVAILABLE = 136
    SERVER_BUSY = 137
    BANNED = 138
    SERVER_SHUTTING_DOWN = 139
    BAD_AUTH_METHOD = 140
    KEEP_ALIVE_TIMEOUT = 141
    SESSION_TAKEN_OVER = 142
    INVALID_TOPIC_FILTER = 143
    INVALID_TOPIC_NAME = 144
    PACKET_ID_IN_USE = 145
    PACKET_ID_NOT_FOUND = 146
    RECEIVE_MAX_EXCEEDED = 147
    INVALID_TOPIC_ALIAS = 148
    PACKET_TOO_LARGE = 149
    MESSAGE_RATE_TOO_HIGH = 150
    QUOTA_EXCEEDED = 151
    ADMINISTRATIVE_ACTION = 152
    INVALID_PAYLOAD_FORMAT = 153
    RETAIN_NOT_SUPPORTED = 154
    QOS_NOT_SUPPORTED = 155
    USE_ANOTHER_SERVER = 156
    SERVER_MOVED = 157
    SHARED_SUBS_NOT_SUPPORTED = 158
    CONNECTION_RATE_EXCEEDED = 159
    MAX_CONNECT_TIME = 160
    SUBSCRIPTION_ID_NOT_SUPPORTED = 161
    WILDCARD_SUBS_NOT_SUPPORTED = 162

    @property
    def is_success(self) -> bool:
        """ Indicates if the reason code represents a success. """
        return self < ReasonCode.UNSPECIFIED_ERROR

    @property
    def is_error(self) -> bool:
        """ Indicate if the reason code represents an error. """
        return self >= ReasonCode.UNSPECIFIED_ERROR


reasons = {
    ReasonCode.NORMAL_DISCONNECT: "Connection is closed.",
    ReasonCode.UNSPECIFIED_ERROR: "An error occurred.",
    ReasonCode.MALFORMED_PACKET: "Malformed packet.",
    ReasonCode.PROTOCOL_ERROR: "Protocol error.",
    ReasonCode.IMPLEMENTATION_ERROR: "Implementation error.",
    ReasonCode.NOT_AUTHORIZED: "Not authorized.",
    ReasonCode.SESSION_TAKEN_OVER: "Session is taken over by another client.",
    ReasonCode.INVALID_TOPIC_FILTER: "Invalid topic filter",
    ReasonCode.PACKET_ID_IN_USE: "Packet identifier is already in use.",
    ReasonCode.QUOTA_EXCEEDED: "Quota exceeded.",
    ReasonCode.INVALID_PAYLOAD_FORMAT: "Payload is not valid UTF-8.",
    ReasonCode.SHARED_SUBS_NOT_SUPPORTED:
        "Shared subscriptions are not supported",
    ReasonCode.SUBSCRIPTION_ID_NOT_SUPPORTED:
        "Subscription identifiers are not supported",
    ReasonCode.WILDCARD_SUBS_NOT_SUPPORTED:
        "Wildcard subscriptions are not supported.",
}


class MQTTVersion(enum.IntEnum):
    """ MQTT version """
    V311 = 4
    V5 = 5


class PacketType(enum.IntEnum):
    """ MQTT Packet type. """
    RESERVED = 0x00
    CONNECT = 0x10
    CONNACK = 0x20
    PUBLISH = 0x30
    PUBACK = 0x40
    PUBREC = 0x50
    PUBREL = 0x60
    PUBCOMP = 0x70
    SUBSCRIBE = 0x80
    SUBACK = 0x90
    UNSUBSCRIBE = 0xA0
    UNSUBACK = 0xB0
    PINGREQ = 0xC0
    PINGRESP = 0xD0
    DISCONNECT = 0xE0
    AUTH = 0xF0
    WILL = 0x100


packet_reason_codes = {
    PacketType.CONNACK: {
        ReasonCode.SUCCESS,
        ReasonCode.UNSPECIFIED_ERROR,
        ReasonCode.MALFORMED_PACKET,
        ReasonCode.PROTOCOL_ERROR,
        ReasonCode.IMPLEMENTATION_ERROR,
        ReasonCode.UNSUPPORTED_PROTOCOL_VERSION,
        ReasonCode.INVALID_CLIENT_ID,
        ReasonCode.BAD_USER_OR_PWD,
        ReasonCode.NOT_AUTHORIZED,
        ReasonCode.SERVER_UNAVAILABLE,
        ReasonCode.SERVER_BUSY,
        ReasonCode.BANNED,
        ReasonCode.BAD_AUTH_METHOD,
        ReasonCode.INVALID_TOPIC_NAME,
        ReasonCode.PACKET_TOO_LARGE,
        ReasonCode.QUOTA_EXCEEDED,
        ReasonCode.INVALID_PAYLOAD_FORMAT,
        ReasonCode.RETAIN_NOT_SUPPORTED,
        ReasonCode.QOS_NOT_SUPPORTED,
        ReasonCode.USE_ANOTHER_SERVER,
        ReasonCode.SERVER_MOVED,
        ReasonCode.CONNECTION_RATE_EXCEEDED,
    },
    PacketType.SUBACK: {
        ReasonCode.GRANTED_QOS_0,
        ReasonCode.GRANTED_QOS_1,
        ReasonCode.GRANTED_QOS_2,
        ReasonCode.UNSPECIFIED_ERROR,
        ReasonCode.IMPLEMENTATION_ERROR,
        ReasonCode.NOT_AUTHORIZED,
        ReasonCode.INVALID_TOPIC_FILTER,
        ReasonCode.PACKET_ID_IN_USE,
        ReasonCode.QUOTA_EXCEEDED,
        ReasonCode.SHARED_SUBS_NOT_SUPPORTED,
        ReasonCode.SUBSCRIPTION_ID_NOT_SUPPORTED,
        ReasonCode.WILDCARD_SUBS_NOT_SUPPORTED,
    },
    PacketType.UNSUBACK: {
        ReasonCode.SUCCESS,
        ReasonCode.NO_SUBSCRIPTION_EXISTED,
        ReasonCode.UNSPECIFIED_ERROR,
        ReasonCode.IMPLEMENTATION_ERROR,
        ReasonCode.NOT_AUTHORIZED,
        ReasonCode.INVALID_TOPIC_FILTER,
        ReasonCode.PACKET_ID_IN_USE,
    },
    PacketType.PUBACK: {
        ReasonCode.SUCCESS,
        ReasonCode.NO_MATCHING_SUBSCRIBERS,
        ReasonCode.UNSPECIFIED_ERROR,
        ReasonCode.IMPLEMENTATION_ERROR,
        ReasonCode.NOT_AUTHORIZED,
        ReasonCode.INVALID_TOPIC_NAME,
        ReasonCode.PACKET_ID_IN_USE,
        ReasonCode.QUOTA_EXCEEDED,
        ReasonCode.INVALID_PAYLOAD_FORMAT,
    },
    PacketType.PUBREC: {
        ReasonCode.SUCCESS,
        ReasonCode.NO_MATCHING_SUBSCRIBERS,
        ReasonCode.UNSPECIFIED_ERROR,
        ReasonCode.IMPLEMENTATION_ERROR,
        ReasonCode.NOT_AUTHORIZED,
        ReasonCode.INVALID_TOPIC_NAME,
        ReasonCode.PACKET_ID_IN_USE,
        ReasonCode.QUOTA_EXCEEDED,
        ReasonCode.INVALID_PAYLOAD_FORMAT,
    },
    PacketType.PUBCOMP: {
        ReasonCode.SUCCESS,
        ReasonCode.PACKET_ID_NOT_FOUND,
    },
}


def verify_reason_code(
    reason_code_val: int, packet_type: PacketType
) -> ReasonCode:
    """ Verifies if a numeric value is a known reason code and valid for the
    packet type.

    """
    try:
        reason_code = ReasonCode(reason_code_val)
    except ValueError as ex:
        raise get_mqtt_ex(
            ReasonCode.MALFORMED_PACKET, "Invalid reason code.") from ex
    if reason_code not in packet_reason_codes[packet_type]:
        raise get_mqtt_ex(
            ReasonCode.PROTOCOL_ERROR, "Invalid reason code for packet."
        )
    return reason_code


class PropertyID(enum.IntEnum):
    """ MQTT Property identifier. """

    PAYLOAD_FORMAT_INDICATOR = 1
    MESSAGE_EXPIRY_INTERVAL = 2
    CONTENT_TYPE = 3
    RESPONSE_TOPIC = 8
    CORRELATION_DATA = 9
    SUBSCRIPTION_ID = 11
    SESSION_EXPIRY_INTERVAL = 17
    ASSIGNED_CLIENT_ID = 18
    SERVER_KEEP_ALIVE = 19
    AUTH_METHOD = 21
    AUTH_DATA = 22
    REQUEST_PROBLEM_INFO = 23
    WILL_DELAY_INTERVAL = 24
    REQUEST_RESPONSE_INFO = 25
    RESPONSE_INFO = 26
    SERVER_REFERENCE = 28
    REASON_STRING = 31
    RECEIVE_MAX = 33
    TOPIC_ALIAS_MAX = 34
    TOPIC_ALIAS = 35
    MAX_QOS = 36
    RETAIN_AVAILABLE = 37
    USER_PROPS = 38
    MAX_PACKET_SIZE = 39
    SUPPORTS_WILDCARDS = 40
    SUBSCRIPTION_ID_AVAILABLE = 41
    SHARED_SUB_AVAILABLE = 42


class PayloadFormat(enum.IntEnum):
    """ MQTT Payload format """
    UNSPECIFIED = 0
    TEXT = 1


class Qos(enum.IntEnum):
    """ MQTT Quality of Service indicator """
    AT_MOST_ONCE = 0
    AT_LEAST_ONCE = 1
    EXACTLY_ONCE = 2


class RetainHandling(enum.IntEnum):
    """ MQTT Retain handling options. """
    ALWAYS = 0
    ONCE = 1
    NEVER = 2


# ==== Exceptions =============================================================


class MQTTException(Exception):
    """ MQTT Exception. """
    args: Tuple[ReasonCode, str]

    @property
    def reason_code(self) -> ReasonCode:
        """ Reason code """
        return self.args[0]

    @property
    def message(self) -> str:
        """ Error Message """
        return self.args[1]


class MalformedPacket(MQTTException):
    """ Malformed Packet exception. """


class ProtocolError(MQTTException):
    """ Protocol Error. """


def get_mqtt_ex(
    reason_code: ReasonCode,
    message: Optional[str] = None,
) -> MQTTException:
    """ Gets the appropriate Exception for the reason code and optional
    message.

    """
    if message is None:
        message = reasons.get(reason_code, "An error occurred.")
    if reason_code is ReasonCode.MALFORMED_PACKET:
        ex_class: Type[MQTTException] = MalformedPacket
    elif reason_code in {
        ReasonCode.PROTOCOL_ERROR,
        ReasonCode.RECEIVE_MAX_EXCEEDED,
        ReasonCode.PACKET_TOO_LARGE,
        ReasonCode.RETAIN_NOT_SUPPORTED,
        ReasonCode.QOS_NOT_SUPPORTED,
        ReasonCode.SHARED_SUBS_NOT_SUPPORTED,
        ReasonCode.SUBSCRIPTION_ID_NOT_SUPPORTED,
        ReasonCode.WILDCARD_SUBS_NOT_SUPPORTED,
    }:
        ex_class = ProtocolError
    else:
        ex_class = MQTTException
    return ex_class(reason_code, message)


# ==== Read routines ==========================================================

def check_valid_string(value: str) -> None:
    """ Check if a string contains invalid characters. """

    # Invalid chars:
    # * C1 control codes (0x00 .. 0x1F)
    # * Delete (0x7F) + C2 control codes (0x7F .. 0x9F)
    # * Non characters (0xFDD0 .. 0xFDEF) and values that have 0xFFFE or 0xFFFF
    #   in the least significant 4 bytes
    # So valid chars are:
    # * Anything between those
    if all(
        (" " <= char < "\x7F")
        or ("\xA0" <= char < "\uFDD0")
        or (
            "\uFDD0" <= char < "\U0010FFFE"
            and (ord(char) & 0xFFFF) not in (0xFFFE, 0xFFFF)
        )
        for char in value
    ):
        return
    raise MalformedPacket(
        ReasonCode.MALFORMED_PACKET, "Invalid characters in UTF 8 string."
    )


# pylint: disable-next=too-few-public-methods
class VariableByteIntReader:
    """ Helper class to read variable byte integers. """
    # class to read a variable byte integer per byte

    MAX_MULTIPLIER = 128**3

    def __init__(self) -> None:
        self._value = 0
        self._multiplier = 1

    def feed(self, value: int) -> Union[None, int]:
        """ Feed a byte value to the parser. """
        self._value += (value & 0x7F) * self._multiplier
        if value & 0x80:
            if self._multiplier == self.MAX_MULTIPLIER:
                raise MalformedPacket(
                    ReasonCode.MALFORMED_PACKET,
                    "Invalid variable byte integer.",
                )
            self._multiplier *= 128
            return None
        return self._value


class UnPacker:
    """ Class to unpack a binary message. """

    def __init__(self, buf: memoryview) -> None:
        self.buf: memoryview = buf
        self.buf_len = len(buf)
        self.pos = 0

    def check_length(self, length: int) -> None:
        """ Check if the necessary number of bytes are available. """
        if self.buf_len - self.pos < length:
            raise MalformedPacket(
                ReasonCode.MALFORMED_PACKET, "Missing data in packet."
            )

    def read_byte(self) -> int:
        """ Read an integer byte value. """
        try:
            val = self.buf[self.pos]
        except IndexError as ex:
            raise MalformedPacket(
                ReasonCode.MALFORMED_PACKET, "Missing data in packet."
            ) from ex
        self.pos += 1
        return val

    def read_bool(self) -> bool:
        """ Read a MQTT boolean. """
        val = self.read_byte()
        if val > 1:
            raise ProtocolError(
                ReasonCode.PROTOCOL_ERROR, "Invalid boolean property value"
            )
        return bool(val)

    def read_format_indicator(self) -> PayloadFormat:
        """ Read a Format Indicator value. """
        payload_val = self.read_byte()
        try:
            return PayloadFormat(payload_val)
        except ValueError as ex:
            raise get_mqtt_ex(
                ReasonCode.MALFORMED_PACKET, "Invalid payload format value."
            ) from ex

    def read_qos(self) -> Qos:
        """ Read a Qos value. """
        qos_val = self.read_byte()
        try:
            return Qos(qos_val)
        except ValueError as ex:
            raise get_mqtt_ex(
                ReasonCode.MALFORMED_PACKET, "Invalid qos value.") from ex

    def read_int(self, length: int) -> int:
        """ Read an MQTT integer of the specified length. """
        self.check_length(length)
        value = int.from_bytes(self.buf[self.pos:self.pos + length], "big")
        self.pos += length
        return value

    def read_int2(self) -> int:
        """ Read an unsigned 2 byte integer. """
        return self.read_int(2)

    def read_int4(self) -> int:
        """ Read an unsigned 4 byte integer. """
        return self.read_int(4)

    def read_buf(self) -> memoryview:
        """ Read a length prepended buffer. """
        str_len = self.read_int2()
        self.check_length(str_len)
        buf = self.buf[self.pos:self.pos + str_len]
        self.pos += str_len
        return buf

    def read_bytes(self) -> bytes:
        """ Reads a MQTT binary value, prepended by 2 bytes length. """
        return bytes(self.read_buf())

    def read_remaining_bytes(self) -> bytes:
        """ Read whatever is left. """
        bin_val = bytes(self.buf[self.pos:])
        self.pos += len(bin_val)
        return bin_val

    def read_string(self) -> str:
        """ Reads a MQTT string, prepended by 2 bytes length. """
        try:
            str_val = decode(self.read_buf())
        except UnicodeError as ex:
            raise get_mqtt_ex(
                ReasonCode.MALFORMED_PACKET, "Invalid UTF-8 string."
            ) from ex
        check_valid_string(str_val)
        return str_val

    def read_string_pair(self) -> Tuple[str, str]:
        """ Reads a MQTT string pair. """
        return self.read_string(), self.read_string()

    def read_var_int(self) -> int:
        """ Reads a variable byte integer. """
        reader = VariableByteIntReader()
        value = None
        while value is None:
            byte_val = self.read_byte()
            value = reader.feed(byte_val)
        return value

    def read_props(self, packet_type: PacketType) -> Dict[PropertyID, Any]:
        """ Reads properties """

        props = {}
        props_len = self.read_var_int()
        end = self.pos + props_len
        while self.pos < end:
            # First read the property id
            prop_id_val = self.read_var_int()
            try:
                # officially a variable byte integer, in practice just a byte
                prop_id = PropertyID(prop_id_val)
            except ValueError as ex:
                raise get_mqtt_ex(
                    ReasonCode.MALFORMED_PACKET,
                    f"Invalid property identifier: '{prop_id_val}'.",
                ) from ex

            # Get info for property
            prop_info = properties[prop_id]

            if packet_type not in prop_info.packet_types:
                # Unexpected property
                raise MalformedPacket(
                    ReasonCode.MALFORMED_PACKET,
                    f"Invalid property type '{prop_id}' for packet "
                    f"'{packet_type}'.",
                )

            # Extract value, using info
            prop_value = prop_info.extract(self)

            if prop_info.appear_once:
                if prop_id in props:
                    raise ProtocolError(
                        ReasonCode.PROTOCOL_ERROR,
                        "Property can only appear once",
                    )
                props[prop_id] = prop_value
            else:
                # For name, value pairs
                if prop_id not in props:
                    props[prop_id] = []
                props[prop_id].append(prop_value)
        return props

    def at_end(self) -> bool:
        """ Indicates if the reader is at the end of the buffer. """
        return self.pos == self.buf_len

    def check_end(self) -> None:
        """ Checks if the reader is at the end of the buffer. """
        if self.pos != self.buf_len:
            raise get_mqtt_ex(ReasonCode.MALFORMED_PACKET, "Too much data.")


# ===== Write routines ========================================================


class Packer:
    """Binary packer for MQTT message components"""

    def __init__(self) -> None:
        self.fmt_parts: List[str] = []
        self.vals: List[Any] = []
        self.size = 0

    def _write_single_val(self, fmt: str, value: Any, size: int) -> None:
        """ Write a single value. """
        self.fmt_parts.append(fmt)
        self.vals.append(value)
        self.size += size

    def write_byte(self, value: int) -> None:
        """ Write an unsigned byte value. """
        self._write_single_val("B", int(value), 1)

    def write_int2(self, value: int) -> None:
        """ Write an unsigned 2 byte integer value. """
        self._write_single_val("H", int(value), 2)

    def write_int4(self, value: int) -> None:
        """ Write an unsigned 4 byte integer value. """
        self._write_single_val("I", int(value), 4)

    def write_bool(self, value: bool) -> None:
        """ Write a boolean value. """
        self.write_byte(bool(value))

    def write_raw_bytes(self, value: bytes) -> None:
        """ Write raw bytes. """
        value = bytes(value)
        len_val = len(value)
        self._write_single_val(f"{len_val}s", value, len_val)

    def write_bytes(self, value: bytes) -> None:
        """ Write a binary value, prepended with the length. """
        value = bytes(value)
        len_val = len(value)
        self.write_int2(len(value))
        self._write_single_val(f"{len_val}s", value, len_val)

    def write_string(self, value: str) -> None:
        """ Write a string, prepended with the length. """
        value = str(value)
        check_valid_string(value)
        self.write_bytes(value.encode())

    def write_string_pair(self, value: Tuple[str, str]) -> None:
        """ Write a MQTT string pair. """
        name, val = value
        self.write_string(name)
        self.write_string(val)

    def calcsize(self) -> int:
        """ Returns the calculated size. """
        return self.size

    def write_var_int(self, value: int) -> None:
        """ Writes a variable byte integer. """
        while True:
            byte_val = value % 128
            value = value // 128
            if value:
                byte_val += 128
            self.write_byte(byte_val)
            if not value:
                break

    def write_props(self, props: Iterable[Tuple[PropertyID, Any]]) -> None:
        """ Writes MQTT properties. """
        # create packer and fill with property values
        props_packer = Packer()  # packer for properties
        for prop_id, prop_val in props:
            prop_info = properties[prop_id]
            if prop_val == prop_info.default:
                # keep size as low as possible, don't write defaults
                continue
            props_packer.write_byte(prop_id)
            prop_info.pack(props_packer, prop_val)

        # write properties length on self
        prop_size = props_packer.calcsize()
        self.write_var_int(prop_size)

        # add property packer formats and values to self
        self.fmt_parts.extend(props_packer.fmt_parts)
        self.vals.extend(props_packer.vals)
        self.size += prop_size

    @property
    def format_string(self) -> str:
        """ Returns the resulting format string. """
        return "".join(self.fmt_parts)


class MessagePacker(Packer):
    """MQTT Message packer

    Call bytes() on an instance to get the binary message, ready for sending
    over the network.

    """
    def __init__(self, packet_type: PacketType, flags: int) -> None:
        super().__init__()
        self.packet_type = packet_type
        self.flags = flags

    def __bytes__(self) -> bytes:
        # create and fill packer for static header
        header_packer = Packer()
        header_packer.write_byte(self.packet_type + self.flags)
        header_packer.write_var_int(self.calcsize())

        # combine header packer and self into network endian bytestring
        return struct.pack(
            "!" + header_packer.format_string + self.format_string,
            *header_packer.vals,
            *self.vals,
        )


# ===== Properties ============================================================


class PropertyInfo(NamedTuple):
    """ MQTT Property meta information """
    extract: Callable[[UnPacker], Any]
    pack: Callable[[Packer, Any], None]
    appear_once: bool
    default: Any
    packet_types: Set[PacketType]


properties = {
    PropertyID.PAYLOAD_FORMAT_INDICATOR: PropertyInfo(
        UnPacker.read_format_indicator,
        Packer.write_bool,
        True,
        PayloadFormat.UNSPECIFIED,
        {PacketType.PUBLISH, PacketType.WILL},
    ),
    PropertyID.MESSAGE_EXPIRY_INTERVAL: PropertyInfo(
        UnPacker.read_int4,
        Packer.write_int4,
        True,
        None,
        {PacketType.WILL, PacketType.PUBLISH},
    ),
    PropertyID.CONTENT_TYPE: PropertyInfo(
        UnPacker.read_string,
        Packer.write_string,
        True,
        None,
        {PacketType.WILL, PacketType.PUBLISH},
    ),
    PropertyID.RESPONSE_TOPIC: PropertyInfo(
        UnPacker.read_string,
        Packer.write_string,
        True,
        None,
        {PacketType.WILL, PacketType.PUBLISH},
    ),
    PropertyID.CORRELATION_DATA: PropertyInfo(
        UnPacker.read_bytes,
        Packer.write_bytes,
        True,
        None,
        {PacketType.WILL, PacketType.PUBLISH},
    ),
    PropertyID.SUBSCRIPTION_ID: PropertyInfo(
        UnPacker.read_var_int,
        Packer.write_var_int,
        False,
        None,
        {PacketType.SUBSCRIBE, PacketType.PUBLISH},
    ),
    PropertyID.SESSION_EXPIRY_INTERVAL: PropertyInfo(
        UnPacker.read_int4,
        Packer.write_int4,
        True,
        0,
        {PacketType.CONNECT, PacketType.CONNACK, PacketType.DISCONNECT},
    ),
    PropertyID.ASSIGNED_CLIENT_ID: PropertyInfo(
        UnPacker.read_string,
        Packer.write_string,
        True,
        None,
        {PacketType.CONNACK},
    ),
    PropertyID.SERVER_KEEP_ALIVE: PropertyInfo(
        UnPacker.read_int2, Packer.write_int2, True, None, {PacketType.CONNACK}
    ),
    PropertyID.AUTH_METHOD: PropertyInfo(
        UnPacker.read_string,
        Packer.write_string,
        True,
        None,
        {PacketType.CONNECT, PacketType.CONNACK, PacketType.AUTH},
    ),
    PropertyID.AUTH_DATA: PropertyInfo(
        UnPacker.read_bytes,
        Packer.write_bytes,
        True,
        None,
        {PacketType.CONNECT, PacketType.CONNACK, PacketType.AUTH},
    ),
    PropertyID.REQUEST_PROBLEM_INFO: PropertyInfo(
        UnPacker.read_bool, Packer.write_bool, True, True, {PacketType.CONNECT}
    ),
    PropertyID.WILL_DELAY_INTERVAL: PropertyInfo(
        UnPacker.read_int4, Packer.write_int4, True, 0, {PacketType.WILL}
    ),
    PropertyID.REQUEST_RESPONSE_INFO: PropertyInfo(
        UnPacker.read_bool,
        Packer.write_bool,
        True,
        False,
        {PacketType.CONNECT},
    ),
    PropertyID.RESPONSE_INFO: PropertyInfo(
        UnPacker.read_string, Packer.write_string, True, None,
        {PacketType.CONNACK},
    ),
    PropertyID.SERVER_REFERENCE: PropertyInfo(
        UnPacker.read_string, Packer.write_string, True, None,
        {PacketType.CONNACK, PacketType.DISCONNECT}
    ),
    PropertyID.REASON_STRING: PropertyInfo(
        UnPacker.read_string,
        Packer.write_string,
        True,
        None,
        {
            PacketType.DISCONNECT,
            PacketType.CONNACK,
            PacketType.PUBACK,
            PacketType.PUBREC,
            PacketType.PUBREL,
            PacketType.PUBCOMP,
            PacketType.SUBACK,
            PacketType.UNSUBACK,
            PacketType.AUTH,
        },
    ),
    PropertyID.RECEIVE_MAX: PropertyInfo(
        UnPacker.read_int2,
        Packer.write_int2,
        True,
        None,
        {PacketType.CONNECT, PacketType.CONNACK},
    ),
    PropertyID.TOPIC_ALIAS_MAX: PropertyInfo(
        UnPacker.read_int2,
        Packer.write_int2,
        True,
        0,
        {PacketType.CONNECT, PacketType.CONNACK},
    ),
    PropertyID.TOPIC_ALIAS: PropertyInfo(
        UnPacker.read_int2,
        Packer.write_int2,
        True,
        None,
        {PacketType.PUBLISH},
    ),
    PropertyID.MAX_QOS: PropertyInfo(
        UnPacker.read_qos, Packer.write_byte, True, Qos.EXACTLY_ONCE,
        {PacketType.CONNACK}
    ),
    PropertyID.RETAIN_AVAILABLE: PropertyInfo(
        UnPacker.read_bool, Packer.write_bool, True, True,
        {PacketType.CONNACK}
    ),
    PropertyID.USER_PROPS: PropertyInfo(
        UnPacker.read_string_pair,
        Packer.write_string_pair,
        False,
        None,
        {
            PacketType.CONNECT,
            PacketType.CONNACK,
            PacketType.PUBLISH,
            PacketType.WILL,
            PacketType.PUBACK,
            PacketType.PUBREC,
            PacketType.PUBREL,
            PacketType.PUBCOMP,
            PacketType.SUBSCRIBE,
            PacketType.SUBACK,
            PacketType.UNSUBSCRIBE,
            PacketType.UNSUBACK,
            PacketType.DISCONNECT,
            PacketType.AUTH,
        },
    ),
    PropertyID.MAX_PACKET_SIZE: PropertyInfo(
        UnPacker.read_int4,
        Packer.write_int4,
        True,
        None,
        {PacketType.CONNECT, PacketType.CONNACK},
    ),
    PropertyID.SUPPORTS_WILDCARDS: PropertyInfo(
        UnPacker.read_bool, Packer.write_bool, True, True, {PacketType.CONNACK}
    ),
    PropertyID.SUBSCRIPTION_ID_AVAILABLE: PropertyInfo(
        UnPacker.read_bool, Packer.write_bool, True, True, {PacketType.CONNACK}
    ),
    PropertyID.SHARED_SUB_AVAILABLE: PropertyInfo(
        UnPacker.read_bool, Packer.write_bool, True, True,
        {PacketType.CONNACK},
    ),
}


# ===== Base Asyncio Protocol Class ===========================================


# pylint: disable-next=too-many-instance-attributes
class BaseProtocol(BufferedProtocol):
    """ Base MQTT Protocoll class. """

    STANDARD_BUF_SIZE = 0x2000

    def __init__(self) -> None:
        # reading buffers and counters
        self._bytes_read = 0
        self._buf = self._standard_buf = memoryview(
            bytearray(self.STANDARD_BUF_SIZE)
        )
        self._msg_part_len = 1
        self._packet_type = PacketType.RESERVED
        self._flags = 0
        self._vb_int: Optional[VariableByteIntReader] = None
        self._transport: Optional[Transport] = None
        self._write_fut = None
        self._loop = asyncio.get_event_loop()
        self._last_sent = self._loop.time()

    def connection_made(
        self, transport: asyncio.BaseTransport
    ) -> None:
        self._transport = transport  # type: ignore

    def connection_lost(self, exc: Union[Exception, None]) -> None:
        self._transport = None

    def get_buffer(self, sizehint: int) -> memoryview:
        """Gets a buffer to receive data into."""
        buf = self._buf
        if self._bytes_read:
            buf = buf[self._bytes_read:]
        return buf

    def buffer_updated(self, nbytes: int) -> None:
        """Data is received"""

        self._bytes_read += nbytes
        msg_start = 0
        msg_part_len: Optional[int]

        while self._bytes_read >= self._msg_part_len:
            # read in three stages, first byte, then remaining length, and then
            # content
            if self._packet_type is PacketType.RESERVED:
                # read first byte
                byte1 = self._standard_buf[msg_start]
                self._flags = byte1 % 16
                try:
                    self._packet_type = PacketType(byte1 - self._flags)
                except ValueError as ex:
                    raise get_mqtt_ex(
                        ReasonCode.PROTOCOL_ERROR, "Unknown packet type"
                    ) from ex
                if self._packet_type == PacketType.RESERVED:
                    raise get_mqtt_ex(
                        ReasonCode.PROTOCOL_ERROR, "Reserved packet"
                    )

                # set up for reading remaining length
                self._vb_int = VariableByteIntReader()
                msg_part_len = 1
            elif self._vb_int is not None:
                # read remaining length
                msg_part_len = self._vb_int.feed(self._standard_buf[msg_start])
                if msg_part_len is None:
                    # need another byte for remaining length
                    msg_part_len = 1
                else:
                    # remaining length established, set up for content
                    self._vb_int = None
                    if msg_part_len > self.STANDARD_BUF_SIZE:
                        # message longer that standard buf, allocate XL buf
                        self._buf = memoryview(bytearray(msg_part_len))
            else:
                # handle the content
                self.handle_message(
                    self._buf[msg_start:msg_start + self._msg_part_len]
                )

                # if XL buffer was used, it is discarded now
                self._buf = self._standard_buf

                # set up for next message
                msg_part_len = 1
                self._packet_type = PacketType.RESERVED

            # set up for reading the next message part
            self._bytes_read -= self._msg_part_len
            msg_start += self._msg_part_len
            self._msg_part_len = msg_part_len

        if self._bytes_read and msg_start:
            # move incomplete trailing message part to start of buffer
            self._buf[: self._bytes_read] = self._standard_buf[
                msg_start:msg_start + self._bytes_read]

    @classmethod
    @lru_cache(maxsize=None)
    def get_handler(
        cls, packet_type: PacketType
    ) -> Callable[["BaseProtocol", UnPacker], None]:
        """ Gets the handler method for the packet type. """
        try:
            handler = getattr(cls, f"handle_{packet_type.name.lower()}_msg")
        except AttributeError as ex:
            raise ProtocolError(
                ReasonCode.PROTOCOL_ERROR,
                f"Invalid packet type '{packet_type.name}' for role.",
            ) from ex
        return handler  # type: ignore

    def handle_message(self, buf: memoryview) -> None:
        """ Handles the incoming message. """
        self.get_handler(self._packet_type)(self, UnPacker(buf))

    def verify_reason_code(self, reason_code_val: int) -> ReasonCode:
        """ Verifies the reason code for the current packet type. """
        return verify_reason_code(reason_code_val, self._packet_type)

    async def write(self, data: bytes) -> None:
        """ Sends data to the server. """
        if self._write_fut is not None:
            await self._write_fut
        if self._transport is None:
            return
        self._transport.write(data)
        self._last_sent = self._loop.time()
