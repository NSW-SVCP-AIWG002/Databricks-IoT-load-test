"""
locustfile.py - Azure IoT Hub (MQTT) → Event Hubs 接続・負荷確認

IoT Hub の MQTT エンドポイントへテレメトリを送信し、
Event Hub 側でメッセージ到達を確認するシナリオ。

本番フロー:
  Locust (MQTT) → IoT Hub → Event Hub → silver_pipeline (Databricks) → ADLS Silver
                                       → Event Hubs Capture           → ADLS Bronze

認証方式:
  - IoT Hub デバイス認証: SAS トークン（対称鍵）
  - デバイス情報: register_devices.py が生成した device_credentials.csv を参照

スケール設計:
  - 1コンテナあたり DEVICE_COUNT 台のデバイスを担当
  - DEVICE_ID_OFFSET でコンテナ間のデバイスID範囲を分離
  - 例) 20コンテナ × 1000台 = 2万台
      コンテナ1: DEVICE_ID_OFFSET=1     → device_id: 1〜1000
      コンテナ2: DEVICE_ID_OFFSET=1001  → device_id: 1001〜2000

事前準備:
  1. register_devices.py を実行して device_credentials.csv を生成
  2. docker-compose で device_credentials.csv をコンテナにマウント
  3. silver_pipeline.py を Databricks で起動

実行例（ヘッドレス）:
  # 1台で疎通確認
  locust --headless -u 2 -r 1 --run-time 2m

  # 1000台 (コンテナ2台目)
  DEVICE_COUNT=1000 DEVICE_ID_OFFSET=1001 locust --headless -u 1001 -r 50 --run-time 10m
"""

import base64
import csv
import hashlib
import hmac
import json
import logging
import selectors
from contextlib import suppress

# azure.eventhub の接続状態ログを抑制（INFO→WARNING に変更）
logging.getLogger("azure.eventhub").setLevel(logging.WARNING)
logging.getLogger("azure.eventhub._pyamqp").setLevel(logging.WARNING)
import os
import queue
import random
import ssl
import threading
import time
import urllib.parse
import uuid
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from azure.eventhub import EventHubConsumerClient
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv
from locust import User, between, events, task
from locust.runners import MasterRunner

load_dotenv()


# ---------------------------------------------------------------------------
# カスタム MQTT クライアント（参考資料 mqtt/client.py ベース、gevent 互換）
# ---------------------------------------------------------------------------

class _MqttGeventClient(mqtt.Client):
    """
    gevent 互換 MQTT クライアント。

    参考資料（reference/locust/worker/mqtt/client.py）を参考に以下を実装:
      - selectors ベースの loop() で gevent の monkey patch との親和性を向上
      - connect_status フラグで接続状態を管理（on_connect コールバックで更新）
      - loop() → _loop() の委譲により loop_start() のバックグラウンド処理と整合

    参考資料との主な違い:
      - Locust イベント通知は MqttDeviceUser 側で行う（本クラスは接続管理のみ）
      - Azure IoT Hub 向け（AWS IoT Core ではない）
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.connect_status = False          # on_connect で True / on_disconnect で False
        self._locust_on_disconnect = None    # 切断時に呼ぶ外部ハンドラ（MqttDeviceUser が設定）

        # 接続状態コールバックを設定
        self.on_connect    = self._on_connect_cb
        self.on_disconnect = self._on_disconnect_cb

    def _on_connect_cb(self, client, userdata, flags, rc):
        """CONNACK 受信時: rc=0 なら接続成功"""
        self.connect_status = (rc == 0)

    def _on_disconnect_cb(self, client, userdata, rc):
        """切断時: connect_status をリセットし、外部ハンドラを呼ぶ"""
        self.connect_status = False
        if self._locust_on_disconnect:
            self._locust_on_disconnect(client, userdata, rc)

    def loop(self, timeout=1.0, max_packets=1):
        """
        selectors ベースの loop 実装（参考資料と同方式）。
        gevent の monkey patch 済み selectors と親和性が高く、
        1,300 グリーンスレッドが同時に呼んでも競合が起きにくい。
        """
        if timeout < 0.0:
            raise ValueError("Invalid timeout.")
        # loop_stop() が呼ばれた場合は即座に終了
        if getattr(self, '_thread_terminate', False):
            return mqtt.MQTT_ERR_NO_CONN
        if self._sock is None:
            return mqtt.MQTT_ERR_NO_CONN
        return self._loop(timeout)

    def _loop(self, timeout=1.0):
        """selectors を使った I/O 多重化（参考資料 _loop() と同実装）"""
        sel = selectors.DefaultSelector()
        eventmask = selectors.EVENT_READ

        # 送信待ちパケットがあれば書き込みも監視
        with suppress(IndexError, AttributeError):
            packet = self._out_packet.popleft()
            self._out_packet.appendleft(packet)
            eventmask |= selectors.EVENT_WRITE

        # SSL ソケットに未処理データがあれば select をスキップして即処理
        pending_bytes = 0
        if hasattr(self._sock, "pending"):
            pending_bytes = self._sock.pending()
        if pending_bytes > 0:
            timeout = 0.0

        try:
            sel.register(self._sock, eventmask)
            sockpairR = getattr(self, '_sockpairR', None)
            if sockpairR is not None and sockpairR != -1:
                sel.register(sockpairR, selectors.EVENT_READ)
            events_ready = sel.select(timeout)
        except TypeError:
            return mqtt.MQTT_ERR_CONN_LOST
        except ValueError:
            return mqtt.MQTT_ERR_CONN_LOST
        except Exception:
            return mqtt.MQTT_ERR_UNKNOWN
        finally:
            sel.close()

        socklist = [[], []]
        for key, _event in events_ready:
            if key.events & selectors.EVENT_READ:
                socklist[0].append(key.fileobj)
            if key.events & selectors.EVENT_WRITE:
                socklist[1].append(key.fileobj)

        if self._sock in socklist[0] or pending_bytes > 0:
            rc = self.loop_read()
            if rc or self._sock is None:
                return rc

        sockpairR = getattr(self, '_sockpairR', None)
        if sockpairR and sockpairR != -1 and sockpairR in socklist[0]:
            # sockpair に書き込まれた通知を処理
            socklist[1].insert(0, self._sock)
            with suppress(BlockingIOError):
                sockpairR.recv(10000)

        if self._sock in socklist[1]:
            rc = self.loop_write()
            if rc or self._sock is None:
                return rc

        return self.loop_misc()


# ---------------------------------------------------------------------------
# デバイス認証情報キュー: test_start で読込み → __init__ で取得
# ---------------------------------------------------------------------------

_CREDS_FILE = os.getenv("DEVICE_CREDENTIALS_FILE", "device_credentials.csv")

# デバイス認証情報 dict を格納: {device_name, device_id, primary_key}
_credential_queue: queue.Queue = queue.Queue()
_registration_done = threading.Event()  # 読込完了まで __init__ をブロック
_connected_count = 0
_failed_count = 0
_issued_count = 0   # credential を取得したユーザー数
_connected_count_lock = threading.Lock()


# ---------------------------------------------------------------------------
# ヘルパー: デバイス認証情報の読み込み
# ---------------------------------------------------------------------------

def _load_credentials(offset: int, count: int) -> list[dict]:
    """
    device_credentials.csv から指定範囲の認証情報を読み込む。
    参考資料の adapter_list.csv に相当する処理。
    """
    if not os.path.exists(_CREDS_FILE):
        raise FileNotFoundError(
            f"{_CREDS_FILE} が見つかりません。"
            "register_devices.py を実行してからテストを開始してください。"
        )

    creds = []
    with open(_CREDS_FILE, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            dev_id = int(row["device_id"])
            if offset <= dev_id < offset + count:
                creds.append({
                    "device_name": row["device_name"],
                    "device_id":   dev_id,
                    "primary_key": row["primary_key"],
                })

    print(f"[デバイス読込] {len(creds)}台完了 "
          f"(offset={offset}, 範囲: {offset}〜{offset + count - 1})")
    return creds


# ---------------------------------------------------------------------------
# ヘルパー: SAS トークン生成
# ---------------------------------------------------------------------------

def _generate_sas_token(
    iothub_hostname: str,
    device_name: str,
    primary_key: str,
    expiry_secs: int = 3600,
) -> str:
    """
    IoT Hub デバイス用 SAS トークンを生成する。
    有効期限: expiry_secs 秒（デフォルト 1時間）
    """
    uri         = f"{iothub_hostname}/devices/{device_name}"
    encoded_uri = urllib.parse.quote(uri, safe="")
    expiry      = int(time.time()) + expiry_secs
    to_sign     = f"{encoded_uri}\n{expiry}"

    key_bytes = base64.b64decode(primary_key)
    sig = base64.b64encode(
        hmac.new(key_bytes, to_sign.encode("utf-8"), hashlib.sha256).digest()
    ).decode("utf-8")
    sig = urllib.parse.quote(sig, safe="")

    return f"SharedAccessSignature sr={encoded_uri}&sig={sig}&se={expiry}"


# ---------------------------------------------------------------------------
# ヘルパー: テレメトリペイロード生成
# ---------------------------------------------------------------------------

_TARGET_MESSAGE_BYTES = 1024  # 実機メッセージサイズに合わせる


def _make_telemetry(device_id: int) -> dict:
    set_temp_f1 = round(random.uniform(-22.0, -18.0), 1)
    set_temp_f2 = round(random.uniform(-27.0, -23.0), 1)
    payload = {
        "device_id": device_id,  # silver_pipeline schema: IntegerType
        "event_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "external_temp": round(random.uniform(-10.0, 40.0), 1),
        # Freezer 1
        "set_temp_freezer_1": set_temp_f1,
        "internal_sensor_temp_freezer_1": round(set_temp_f1 + random.uniform(-0.5, 0.5), 1),
        "internal_temp_freezer_1": round(set_temp_f1 + random.uniform(-0.8, 0.8), 1),
        "df_temp_freezer_1": round(set_temp_f1 + random.uniform(3.0, 6.0), 1),
        "condensing_temp_freezer_1": round(random.uniform(30.0, 45.0), 1),
        "adjusted_internal_temp_freezer_1": round(set_temp_f1 + random.uniform(-0.5, 0.5), 1),
        # Freezer 2
        "set_temp_freezer_2": set_temp_f2,
        "internal_sensor_temp_freezer_2": round(set_temp_f2 + random.uniform(-0.5, 0.5), 1),
        "internal_temp_freezer_2": round(set_temp_f2 + random.uniform(-0.8, 0.8), 1),
        "df_temp_freezer_2": round(set_temp_f2 + random.uniform(3.0, 6.0), 1),
        "condensing_temp_freezer_2": round(random.uniform(33.0, 48.0), 1),
        "adjusted_internal_temp_freezer_2": round(set_temp_f2 + random.uniform(-0.5, 0.5), 1),
        # モーター類
        "compressor_freezer_1": round(random.uniform(2500.0, 3200.0), 1),
        "compressor_freezer_2": round(random.uniform(2700.0, 3400.0), 1),
        "fan_motor_1": round(random.uniform(1100.0, 1300.0), 1),
        "fan_motor_2": round(random.uniform(1100.0, 1300.0), 1),
        "fan_motor_3": round(random.uniform(1100.0, 1300.0), 1),
        "fan_motor_4": round(random.uniform(1100.0, 1300.0), 1),
        "fan_motor_5": round(random.uniform(1100.0, 1300.0), 1),
        # デフロストヒーター（通常はOFF=0、稀に動作）
        "defrost_heater_output_1": round(random.choices(
            [0.0, random.uniform(50.0, 150.0)], weights=[0.95, 0.05])[0], 1),
        "defrost_heater_output_2": round(random.choices(
            [0.0, random.uniform(50.0, 150.0)], weights=[0.95, 0.05])[0], 1),
    }
    # 実機メッセージサイズ (1KB) に合わせてパディング
    pad_size = (_TARGET_MESSAGE_BYTES
                - len(json.dumps(payload).encode("utf-8"))
                - len(',"_padding":""'))
    if pad_size > 0:
        payload["_padding"] = "x" * pad_size
    return payload


# ---------------------------------------------------------------------------
# 負荷検証シナリオ: IoT Hub (MQTT) 経由送信
# ---------------------------------------------------------------------------

class MqttDeviceUser(User):
    """
    IoT Hub の MQTT エンドポイントへテレメトリを送信する。

    参考資料（reference/locust/worker/locustfile.py）の設計に合わせた実装:
      - MQTT 接続を __init__（コンストラクタ）で開始
        → gevent スケジューラ開始前に接続処理を行い、CONNACK 競合を回避
      - loop_start() でバックグラウンドスレッドが CONNACK・keepalive を処理
        → on_start() では sleep(1.0) で待機するだけで loop() 呼び出し不要
      - connect_status フラグで接続状態を管理（on_connect コールバック）
      - keepalive=600（参考資料と同値）
    """

    # 送信間隔（60〜120秒）
    wait_time = between(60, 120)

    def _on_disconnect(self, client, userdata, rc):
        """切断コールバック: 予期しない切断を Locust Web UI に報告"""
        if rc != 0:
            print(f"[MQTT] 予期しない切断 device={self.device_name} rc={rc}")
            self.environment.events.request.fire(
                request_type="MQTT",
                name="disconnect",
                response_time=0,
                response_length=0,
                exception=Exception(f"unexpected disconnect rc={rc}"),
                context=self.context(),
            )

    def __init__(self, environment):
        """
        参考資料と同方式: __init__ で MQTT 接続を開始する。
        gevent のタスクスケジューラが動き出す前に接続処理を行うことで、
        1,300 台の同時 CONNACK 待機による gevent 過負荷を回避する。
        """
        super().__init__(environment)

        self._no_device = False  # デバイス未割り当てフラグ

        # 認証情報読込完了まで待機
        _registration_done.wait()

        global _issued_count
        try:
            cred = _credential_queue.get_nowait()
            with _connected_count_lock:
                _issued_count += 1
        except queue.Empty:
            print("[WARN] デバイスキューが空です。このユーザーを停止します。")
            self._no_device = True
            return

        self.device_name  = cred["device_name"]
        self.device_id    = cred["device_id"]
        self._primary_key = cred["primary_key"]

        iothub_hostname = os.environ.get("IOTHUB_HOSTNAME", "")
        if not iothub_hostname:
            raise EnvironmentError("IOTHUB_HOSTNAME が設定されていません")
        self._iothub_hostname = iothub_hostname
        self._topic = f"devices/{self.device_name}/messages/events/"

        # SAS トークン生成（有効期限: 24時間）
        sas_token = _generate_sas_token(
            iothub_hostname, self.device_name, self._primary_key, expiry_secs=86400
        )

        # カスタム MQTT クライアント生成（selectors ベース loop + connect_status 管理）
        self._mqtt = _MqttGeventClient(
            client_id=self.device_name,
            protocol=mqtt.MQTTv311,
            transport="tcp",
        )
        self._mqtt._locust_on_disconnect = self._on_disconnect
        self._mqtt.username_pw_set(
            username=f"{iothub_hostname}/{self.device_name}/?api-version=2021-04-12",
            password=sas_token,
        )
        self._mqtt.tls_set(
            cert_reqs=ssl.CERT_REQUIRED,
            tls_version=ssl.PROTOCOL_TLS_CLIENT,
        )
        # __init__ で接続開始（参考資料と同方式）
        # keepalive=600: 参考資料と同値。wait_time(max120秒)より十分長い
        try:
            self._mqtt.connect(iothub_hostname, port=8883, keepalive=600)
            self._mqtt.loop_start()  # バックグラウンドスレッドで CONNACK・keepalive を処理
            print(f"[MQTT] 接続開始 device={self.device_name}")
        except Exception as e:
            print(f"[MQTT] 接続失敗 device={self.device_name}: {e}")
            # 失敗してもユーザーは生成し、on_start() で connect_status を確認する

    def on_start(self):
        """
        参考資料と同方式: on_start() では接続完了を待つだけ。
        loop_start() のバックグラウンドスレッドが CONNACK を処理するため
        loop() の呼び出しは不要。sleep(1.0) で yield することで
        バックグラウンドスレッドに CPU を譲る。
        """
        if self._no_device:
            self.stop()
            return

        # connect_status が True になるまで待機（参考資料の connect_status チェックと同方式）
        connack_timeout = 300
        connack_start = time.time()
        while not self._mqtt.connect_status and time.time() - connack_start < connack_timeout:
            time.sleep(1.0)  # gevent が他のグリーンスレッドに CPU を譲る

        if not self._mqtt.connect_status:
            print(f"[MQTT] CONNACKタイムアウト device={self.device_name} → 停止")
            self.stop()
            return

        with _connected_count_lock:
            global _connected_count
            _connected_count += 1
            print(f"[接続完了] {self.device_name} 接続済み: {_connected_count}台 / {_issued_count}台")

        # 送信開始タイミングを分散（一斉送信を防ぐ）
        time.sleep(random.uniform(0, 60))

    def on_stop(self):
        if hasattr(self, "_mqtt"):
            self._mqtt.loop_stop()
            self._mqtt.disconnect()
            print(f"[MQTT] 切断 device={self.device_name}")

    @task
    def send_telemetry(self):
        body = json.dumps(_make_telemetry(self.device_id)).encode("utf-8")

        # 接続が切れている場合は再接続を試みる
        if not self._mqtt.connect_status:
            print(f"[MQTT] 切断検知 device={self.device_name} → 再接続試行")
            time.sleep(random.uniform(0, 30))
            for retry in range(1, 6):
                try:
                    self._mqtt.reconnect()
                    # loop_start() は起動済みのため再呼び出し不要
                    reconnack_start = time.time()
                    while not self._mqtt.connect_status and time.time() - reconnack_start < 30:
                        time.sleep(1.0)
                    if not self._mqtt.connect_status:
                        raise RuntimeError("再接続 CONNACKタイムアウト")
                    print(f"[MQTT] 再接続成功 device={self.device_name} attempt={retry}")
                    break
                except Exception as reconnect_exc:
                    print(f"[MQTT] 再接続失敗 device={self.device_name} attempt={retry}/5: {reconnect_exc}")
                    if retry == 5:
                        self.environment.events.request.fire(
                            request_type="MQTT",
                            name="send_telemetry",
                            response_time=0,
                            response_length=0,
                            exception=reconnect_exc,
                            context=self.context(),
                        )
                        return
                    time.sleep(5 * retry)

        start = time.perf_counter()
        try:
            self._mqtt.publish(self._topic, payload=body, qos=0)
            # loop_start() がバックグラウンドで送信処理するため loop() 呼び出し不要
            elapsed_ms = int((time.perf_counter() - start) * 1000)
            self.environment.events.request.fire(
                request_type="MQTT",
                name="send_telemetry",
                response_time=elapsed_ms,
                response_length=len(body),
                exception=None,
                context=self.context(),
            )
        except Exception as exc:
            elapsed_ms = int((time.perf_counter() - start) * 1000)
            print(f"[MQTT] 送信失敗 device={self.device_name}: {exc}")
            self.environment.events.request.fire(
                request_type="MQTT",
                name="send_telemetry",
                response_time=elapsed_ms,
                response_length=0,
                exception=exc,
                context=self.context(),
            )


# ---------------------------------------------------------------------------
# 旧シナリオ: Event Hub Kafka 直接送信（IoT Hub をバイパス）
# ---------------------------------------------------------------------------
# ※ 本番フローは MqttDeviceUser（IoT Hub 経由）を使用する。
#    本シナリオは IoT Hub を経由せず Event Hub へ直接送信するため
#    Event Hub 以降のパイプライン単体検証に使用する場合のみアンコメントすること。
# ---------------------------------------------------------------------------

# import itertools
# from confluent_kafka import Producer
# ...（省略）


# ---------------------------------------------------------------------------
# 接続確認シナリオ: Event Hub コンシューマー（変更なし）
# ---------------------------------------------------------------------------

class EventHubConsumerUser(User):
    """
    Event Hub に接続し、シーケンス番号ポーリングでメッセージ到達を確認する。
    IoT Hub → Event Hub 経由でデータが届いているかを監視する。

    必須環境変数:
      EVENTHUB_CONNECTION_STRING - Event Hub 接続文字列
      EVENTHUB_NAME              - Event Hub 名 (EntityPath がない場合)
    """

    fixed_count = 1  # 常に1台のみ起動（監視用）
    wait_time = between(5, 10)

    @staticmethod
    def _in_thread(func, *args):
        """gevent の monkey-patch 外の OS スレッドで実行し AMQP 応答の破壊を防ぐ。"""
        import gevent
        return gevent.get_hub().threadpool.spawn(func, *args).get()

    def on_start(self):
        conn_str = os.getenv("EVENTHUB_CONNECTION_STRING", "")
        if not conn_str:
            raise EnvironmentError("EVENTHUB_CONNECTION_STRING が設定されていません")

        eventhub_name = os.getenv("EVENTHUB_NAME", "")
        has_entity_path = "EntityPath=" in conn_str
        kwargs = {} if has_entity_path else ({"eventhub_name": eventhub_name} if eventhub_name else {})

        print(f"[EventHub] 接続中... (name={eventhub_name or '(EntityPath から取得)'})")
        self.consumer = EventHubConsumerClient.from_connection_string(
            conn_str, consumer_group="$Default", **kwargs
        )

        props = self._in_thread(self.consumer.get_eventhub_properties)
        self.partition_ids = props["partition_ids"]
        print(
            f"[EventHub] 接続成功: name={props['eventhub_name']}, "
            f"パーティション数={len(self.partition_ids)}"
        )

        self.last_seq = {}
        for pid in self.partition_ids:
            pp = self._in_thread(self.consumer.get_partition_properties, pid)
            self.last_seq[pid] = pp["last_enqueued_sequence_number"]
        print(f"[EventHub] 初期シーケンス番号: {self.last_seq}")

    def on_stop(self):
        if hasattr(self, "consumer"):
            self.consumer.close()
            print("[EventHub] 切断")

    @task
    def check_throughput(self):
        """全パーティションのシーケンス番号を取得し、前回から増えた件数を報告する。"""
        total_new = 0
        start = time.perf_counter()
        try:
            for pid in self.partition_ids:
                pp = self._in_thread(self.consumer.get_partition_properties, pid)
                current_seq = pp["last_enqueued_sequence_number"]
                delta = max(0, current_seq - self.last_seq[pid])
                total_new += delta
                self.last_seq[pid] = current_seq

            elapsed_ms = int((time.perf_counter() - start) * 1000)
            print(f"[EventHub] 新着: {total_new}件, {elapsed_ms}ms, seq={self.last_seq}")
            self.environment.events.request.fire(
                request_type="EventHub",
                name="check_throughput",
                response_time=elapsed_ms,
                response_length=total_new,
                exception=None,
                context=self.context(),
            )
        except Exception as exc:
            elapsed_ms = int((time.perf_counter() - start) * 1000)
            print(f"[EventHub] 確認失敗: {exc}")
            self.environment.events.request.fire(
                request_type="EventHub",
                name="check_throughput",
                response_time=elapsed_ms,
                response_length=0,
                exception=exc,
                context=self.context(),
            )


# ---------------------------------------------------------------------------
# イベントフック
# ---------------------------------------------------------------------------

@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    print("=== 負荷検証テスト 開始 ===")

    # 分散モードのマスターはユーザーを実行しないので読込不要
    if isinstance(environment.runner, MasterRunner):
        print("[デバイス読込] マスターノード → スキップ")
        _registration_done.set()
        return

    # DEVICE_COUNT を決定
    from locust.runners import WorkerRunner
    if isinstance(environment.runner, WorkerRunner):
        device_count = int(os.environ["DEVICE_COUNT"])
    else:
        device_count_env = os.getenv("DEVICE_COUNT")
        if device_count_env is not None:
            device_count = int(device_count_env)
        else:
            total_users = environment.parsed_options.num_users or 2
            device_count = max(1, total_users - 1)

    offset = int(os.getenv("DEVICE_ID_OFFSET", "1"))
    print(f"[デバイス読込] 対象: {device_count}台 (offset={offset})")

    # device_credentials.csv から認証情報を読み込みキューに積む
    try:
        creds = _load_credentials(offset, device_count)
    except FileNotFoundError as e:
        print(f"[ERROR] {e}", flush=True)
        raise

    for cred in creds:
        _credential_queue.put(cred)

    _registration_done.set()


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    print("=== 負荷検証テスト 終了 ===")
