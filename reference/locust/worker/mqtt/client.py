# --------------------------------------------------------------------
# locust-plugins のソースコードを複製して、独自仕様を追加しています
# --------------------------------------------------------------------

from __future__ import annotations

import random
import time
import typing
import logging
import base64
import math
import selectors
from contextlib import suppress

from locust.env import Environment

import paho.mqtt.client as mqtt

# Error values
MQTT_ERR_AGAIN = -1
MQTT_ERR_SUCCESS = 0
MQTT_ERR_NOMEM = 1
MQTT_ERR_PROTOCOL = 2
MQTT_ERR_INVAL = 3
MQTT_ERR_NO_CONN = 4
MQTT_ERR_CONN_REFUSED = 5
MQTT_ERR_NOT_FOUND = 6
MQTT_ERR_CONN_LOST = 7
MQTT_ERR_TLS = 8
MQTT_ERR_PAYLOAD_SIZE = 9
MQTT_ERR_NOT_SUPPORTED = 10
MQTT_ERR_AUTH = 11
MQTT_ERR_ACL_DENIED = 12
MQTT_ERR_UNKNOWN = 13
MQTT_ERR_ERRNO = 14
MQTT_ERR_QUEUE_SIZE = 15
MQTT_ERR_KEEPALIVE = 16

if typing.TYPE_CHECKING:
    from paho.mqtt.properties import Properties
    from paho.mqtt.subscribeoptions import SubscribeOptions

# Logger取得
logger = logging.getLogger("user.adapter")

# A SUBACK response for MQTT can only contain 0x00, 0x01, 0x02, or 0x80. 0x80
# indicates a failure to subscribe.
#
# http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Figure_3.26_-
SUBACK_FAILURE = 0x80
REQUEST_TYPE = "MQTT"


def _generate_log_message(thing: str, event: str, result: str, topic: str, msg_id: str, response_time: str):
    logger.info('%s,%s,%s,%s,%s,%s', thing, event, result, topic, msg_id, response_time)
    

def _generate_random_id(length: int, alphabet: str = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"):
    """Generate a random ID from the given alphabet.

    Args:
        length: the number of random characters to generate.
        alphabet: the pool of random characters to choose from.
    """
    return "".join(random.choice(alphabet) for _ in range(length))


def _generate_mqtt_event_name(event_type: str, qos: int, topic: str):
    """Generate a name to identify publish/subscribe tasks.

    This will be used to ultimately identify tasks in the Locust web console.
    This will identify publish/subscribe tasks with their QoS & associated
    topic.

    Examples:
        publish:0:my/topic
        subscribe:1:my/other/topic

    Args:
        event_type: The type of MQTT event (subscribe or publish)
        qos: The quality-of-service associated with this event
        topic: The MQTT topic associated with this event
    """
    return f"{event_type}:{qos}:{topic}"


class PublishedMessageContext(typing.NamedTuple):
    """Stores metadata about outgoing published messages."""

    qos: int
    topic: str
    start_time: float
    payload_size: int


class SubscribeContext(typing.NamedTuple):
    """Stores metadata about outgoing published messages."""

    qos: int
    topic: str
    start_time: float

    
class MqttClient(mqtt.Client):
    def __init__(
        self,
        *args,
        environment: Environment,
        client_id: typing.Optional[str] = None,
        **kwargs,
    ):
        """Initializes a paho.mqtt.Client for use in Locust swarms.

        This class passes most args & kwargs through to the underlying
        paho.mqtt constructor.

        Args:
            environment: the Locust environment with which to associate events.
            client_id: the MQTT Client ID to use in connecting to the broker.
                If not set, one will be randomly generated.
        """
        # If a client ID is not provided, this class will randomly generate an ID
        # of the form: `locust-[0-9a-zA-Z]{16}` (i.e., `locust-` followed by 16
        # random characters, so that the resulting client ID does not exceed the
        # specification limit of 23 characters).

        # This is done in this wrapper class so that this locust client can
        # self-identify when firing requests, since some versions of MQTT will
        # have the broker assign IDs to clients that do not provide one: in this
        # case, there is no way to retrieve the client ID.

        # See https://github.com/eclipse/paho.mqtt.python/issues/237
        if not client_id:
            self.client_id = f"locust-{_generate_random_id(16)}"
        else:
            self.client_id = client_id

        super().__init__(*args, client_id=self.client_id, **kwargs)
        self.environment = environment
        self.on_publish = self._on_publish_cb
        self.on_subscribe = self._on_subscribe_cb
        self.on_disconnect = self._on_disconnect_cb
        self.on_connect = self._on_connect_cb
        # コールバック追加
        self.on_message = self._on_message_cb

        self._publish_requests: dict[int, PublishedMessageContext] = {}
        self._subscribe_requests: dict[int, SubscribeContext] = {}
        self._request_messages: dict[str, PublishedMessageContext] = {}
        
        # 接続ステータスを追加
        self.connect_status = False
        # 応答ステータスを追加
        self.response_status = False

    def _loop(self, timeout: float = 1.0) -> int:
        if timeout < 0.0:
            raise ValueError("Invalid timeout.")

        sel = selectors.DefaultSelector()

        eventmask = selectors.EVENT_READ

        with suppress(IndexError):
            packet = self._out_packet.popleft()
            self._out_packet.appendleft(packet)
            eventmask = selectors.EVENT_WRITE | eventmask

        # used to check if there are any bytes left in the (SSL) socket
        pending_bytes = 0
        if hasattr(self._sock, "pending"):
            pending_bytes = self._sock.pending()

        # if bytes are pending do not wait in select
        if pending_bytes > 0:
            timeout = 0.0

        try:
            if self._sockpairR is None:
                sel.register(self._sock, eventmask)
            else:
                sel.register(self._sock, eventmask)
                sel.register(self._sockpairR, selectors.EVENT_READ)

            events = sel.select(timeout)

        except TypeError:
            # Socket isn't correct type, in likelihood connection is lost
            return int(MQTT_ERR_CONN_LOST)
        except ValueError:
            # Can occur if we just reconnected but rlist/wlist contain a -1 for
            # some reason.
            return int(MQTT_ERR_CONN_LOST)
        except Exception:
            # Note that KeyboardInterrupt, etc. can still terminate since they
            # are not derived from Exception
            return int(MQTT_ERR_UNKNOWN)

        socklist: list[list] = [[], []]

        for key, _event in events:
            if key.events & selectors.EVENT_READ:
                socklist[0].append(key.fileobj)

            if key.events & selectors.EVENT_WRITE:
                socklist[1].append(key.fileobj)

        if self._sock in socklist[0] or pending_bytes > 0:
            rc = self.loop_read()
            if rc or self._sock is None:
                return int(rc)

        if self._sockpairR and self._sockpairR in socklist[0]:
            # Stimulate output write even though we didn't ask for it, because
            # at that point the publish or other command wasn't present.
            socklist[1].insert(0, self._sock)
            # Clear sockpairR - only ever a single byte written.
            with suppress(BlockingIOError):
                # Read many bytes at once - this allows up to 10000 calls to
                # publish() inbetween calls to loop().
                self._sockpairR.recv(10000)

        if self._sock in socklist[1]:
            rc = self.loop_write()
            if rc or self._sock is None:
                return int(rc)

        sel.close()

        return int(self.loop_misc())

    def _on_publish_cb(
        self,
        client: mqtt.Client,
        userdata: typing.Any,
        mid: int,
    ):
        cb_time = time.time()
        try:
            request_context = self._publish_requests.pop(mid)
        except KeyError:
            # we shouldn't hit this block of code
            self.environment.events.request.fire(
                request_type=REQUEST_TYPE,
                name="publish",
                response_time=0,
                response_length=0,
                exception=AssertionError(f"Could not find message data for mid '{mid}' in _on_publish_cb."),
                context={
                    "client_id": self.client_id,
                    "mid": mid,
                },
            )
        else:
            # fire successful publish event
            self.environment.events.request.fire(
                request_type=REQUEST_TYPE,
                #name=_generate_mqtt_event_name("publish", request_context.qos, request_context.topic),
                name="publish",
                response_time=(cb_time - request_context.start_time) * 1000,
                response_length=request_context.payload_size,
                exception=None,
                context={
                    "client_id": self.client_id,
                    **request_context._asdict(),
                },
            )

    def _on_subscribe_cb(
        self,
        client: mqtt.Client,
        userdata: typing.Any,
        mid: int,
        granted_qos: list[int],
    ):
        cb_time = time.time()
        result = "Success"
        try:
            request_context = self._subscribe_requests.pop(mid)
        except KeyError:
            # we shouldn't hit this block of code
            self.environment.events.request.fire(
                request_type=REQUEST_TYPE,
                name="subscribe",
                response_time=0,
                response_length=0,
                exception=AssertionError(f"Could not find message data for mid '{mid}' in _on_subscribe_cb."),
                context={
                    "client_id": self.client_id,
                    "mid": mid,
                },
            )
            result = "Failure"
        else:
            if SUBACK_FAILURE in granted_qos:
                self.environment.events.request.fire(
                    request_type=REQUEST_TYPE,
                    #name=_generate_mqtt_event_name("subscribe", request_context.qos, request_context.topic),
                    name="subscribe",
                    response_time=(cb_time - request_context.start_time) * 1000,
                    response_length=0,
                    exception=AssertionError(f"Broker returned an error response during subscription: {granted_qos}"),
                    context={
                        "client_id": self.client_id,
                        **request_context._asdict(),
                    },
                )
                result = "Failure"
            else:
                # fire successful subscribe event
                self.environment.events.request.fire(
                    request_type=REQUEST_TYPE,
                    #name=_generate_mqtt_event_name("subscribe", request_context.qos, request_context.topic),
                    name="subscribe",
                    response_time=(cb_time - request_context.start_time) * 1000,
                    response_length=0,
                    exception=None,
                    context={
                        "client_id": self.client_id,
                        **request_context._asdict(),
                    },
                )
                result = "Success"
                
        _generate_log_message(self.client_id, "Subscribe", result, request_context.topic, "-", "-")

    def _on_disconnect_cb(
        self,
        client: mqtt.Client,
        userdata: typing.Any,
        rc: int,
    ):
        result = "Success"

        # デバッグ用。切断の原因を表示
        print(rc)
        if rc != 0:
            self.environment.events.request.fire(
                request_type=REQUEST_TYPE,
                name="disconnect",
                response_time=0,
                response_length=0,
                exception=rc,
                context={
                    "client_id": self.client_id,
                },
            )
            result = "Failure"
        else:
            self.environment.events.request.fire(
                request_type=REQUEST_TYPE,
                name="disconnect",
                response_time=0,
                response_length=0,
                exception=None,
                context={
                    "client_id": self.client_id,
                },
            )
            result = "Success"

        # 接続ステータスを追加
        self.connect_status = False
        
        _generate_log_message(self.client_id, "Disconnect", result, "-", "-", "-")

    def _on_connect_cb(
        self,
        client: mqtt.Client,
        userdata: typing.Any,
        flags: dict[str, int],
        rc: int,
    ):
        result = "Success"
        if rc != 0:
            self.environment.events.request.fire(
                request_type=REQUEST_TYPE,
                name="connect",
                response_time=0,
                response_length=0,
                exception=rc,
                context={
                    "client_id": self.client_id,
                },
            )
            # 接続ステータスを追加
            self.connect_status = False
            result = "Failure"
        else:
            self.environment.events.request.fire(
                request_type=REQUEST_TYPE,
                name="connect",
                response_time=0,
                response_length=0,
                exception=None,
                context={
                    "client_id": self.client_id,
                },
            )
            # 接続ステータスを追加
            self.connect_status = True
            result = "Success"
        
        _generate_log_message(self.client_id, "Connect", result, "-", "-", "-")

    def publish(
        self,
        topic: str,
        payload: typing.Optional[bytes] = None,
        qos: int = 0,
        retain: bool = False,
        properties: typing.Optional[Properties] = None,
    ):
        """Publish a message to the MQTT broker.

        This method wraps the underlying paho-mqtt client's method in order to
        set up & fire Locust events.
        """

        request_context = PublishedMessageContext(
            qos=qos,
            topic=topic,
            start_time=time.time(),
            payload_size=len(payload) if payload else 0,
        )

        publish_info = super().publish(topic, payload=payload, qos=qos, retain=retain)
        msg_type = str(int.from_bytes(bytes.fromhex(base64.b64decode(payload).hex()[2:4]), byteorder="little") & 0x7F)
        msg_id = str(int.from_bytes(bytes.fromhex(base64.b64decode(payload).hex()[12:16]), byteorder="little"))

        result = "Success"
        rc = False
        if publish_info.rc != mqtt.MQTT_ERR_SUCCESS:
            self.environment.events.request.fire(
                request_type=REQUEST_TYPE,
                #name=_generate_mqtt_event_name("publish", request_context.qos, request_context.topic),
                name="publish",
                response_time=0,
                response_length=0,
                exception=publish_info.rc,
                context={
                    "client_id": self.client_id,
                    **request_context._asdict(),
                },
            )
            result = "Failure"
        else:
            # store this for use in the on_publish callback
            self._publish_requests[publish_info.mid] = request_context
            self._request_messages[msg_type + '/' + msg_id] = request_context
            result = "Success"
            rc = True

        # 応答ステータスを追加
        self.response_status = False

        _generate_log_message(self.client_id, "Publish", result, topic, msg_id, "-")

        return rc

    def subscribe(
        self,
        topic: str,
        qos: int = 0,
        options: typing.Optional[SubscribeOptions] = None,
        properties: typing.Optional[Properties] = None,
    ):
        """Subscribe to a given topic.

        This method wraps the underlying paho-mqtt client's method in order to
        set up & fire Locust events.
        """
        request_context = SubscribeContext(
            qos=qos,
            topic=topic,
            start_time=time.time(),
        )

        result, mid = super().subscribe(topic=topic, qos=qos)

        if result != mqtt.MQTT_ERR_SUCCESS:
            self.environment.events.request.fire(
                request_type=REQUEST_TYPE,
                #name=_generate_mqtt_event_name("subscribe", request_context.qos, request_context.topic),
                name="subscribe",
                response_time=0,
                response_length=0,
                exception=result,
                context={
                    "client_id": self.client_id,
                    **request_context._asdict(),
                },
            )
        else:
            self._subscribe_requests[mid] = request_context


    def _on_message_cb(
        self,
        client: mqtt.Client,
        userdata: typing.Any,
        message: typing.Any,
    ):

        cb_time = time.time()
        self.environment.events.request.fire(
            request_type=REQUEST_TYPE,
            #name=_generate_mqtt_event_name("response", message.qos, message.topic),
            name="response",
            response_time=0,
            response_length=0,
            exception=None,
            context={
                "client_id": self.client_id,
            },
        )
            
        # 応答ステータスを追加
        self.response_status = True

        msg_type = str(int.from_bytes(bytes.fromhex(base64.b64decode(message.payload).hex()[2:4]), byteorder="little") & 0x7F)
        msg_id = str(int.from_bytes(bytes.fromhex(base64.b64decode(message.payload).hex()[12:16]), byteorder="little"))
        result = "Success"
        response_time = "-"
        try:
            request_context = self._request_messages.pop(msg_type + '/' + msg_id)
        except KeyError:
            result = "Failure"
            response_time = "-"
        else:
            result = "Success"
            response_time = str(math.ceil((cb_time - request_context.start_time) * 1000))
            
        # デバッグ用。ちゃんと通知を遅れたか判定
        res = "OK"
        if base64.b64decode(message.payload).hex()[4:6] != "00":
            res = "NG"
        # print(res)
        # print(res + "\r\n" + base64.b64decode(message.payload).hex())
        
        # with open('Response.log', 'a') as file:
        #     restype = base64.b64decode(message.payload).hex()[3:4]
        #     logStr = str(datetime.now()) + ',' + restype + ':' + res + "\r\n" + base64.b64decode(message.payload).hex()
        #     file.writelines(logStr)

        _generate_log_message(self.client_id, "Response", result, message.topic, msg_id, response_time)
        