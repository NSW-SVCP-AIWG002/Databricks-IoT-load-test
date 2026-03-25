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
# デバイス認証情報キュー: test_start で読込み → on_start で取得
# ---------------------------------------------------------------------------

_CREDS_FILE = os.getenv("DEVICE_CREDENTIALS_FILE", "device_credentials.csv")

# デバイス認証情報 dict を格納: {device_name, device_id, primary_key}
_credential_queue: queue.Queue = queue.Queue()
_registration_done = threading.Event()  # 読込完了まで on_start をブロック


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
# SAS トークン生成
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
    本番フロー: Locust → IoT Hub → Event Hub → silver_pipeline → ADLS

    認証: SAS トークン（device_credentials.csv の primary_key から生成）

    必須環境変数:
      IOTHUB_HOSTNAME           - IoT Hub ホスト名

    スケール関連の環境変数:
      DEVICE_COUNT              - 送信するデバイス台数
      DEVICE_ID_OFFSET          - デバイスID採番の開始オフセット（デフォルト: 1）
      DEVICE_CREDENTIALS_FILE   - デバイス認証情報 CSV パス（デフォルト: device_credentials.csv）
    """

    # 2回目以降は5分間隔で送信
    wait_time = between(300, 300)

    def on_start(self):
        _registration_done.wait()  # 認証情報読込完了まで待機
        # 起動後5分以内のランダムなタイミングで初回送信
        self._first_send = True
        self._initial_wait = random.uniform(0, 300)

        try:
            cred = _credential_queue.get_nowait()
        except queue.Empty:
            raise RuntimeError(
                "デバイスキューが空です。DEVICE_COUNT または -u の値、"
                "または device_credentials.csv の内容を確認してください。"
            )

        self.device_name  = cred["device_name"]
        self.device_id    = cred["device_id"]
        self._primary_key = cred["primary_key"]

        iothub_hostname = os.environ.get("IOTHUB_HOSTNAME", "")
        if not iothub_hostname:
            raise EnvironmentError("IOTHUB_HOSTNAME が設定されていません")
        self._iothub_hostname = iothub_hostname

        # SAS トークン生成（有効期限: 2時間）
        sas_token = _generate_sas_token(
            iothub_hostname, self.device_name, self._primary_key, expiry_secs=7200
        )

        # paho-mqtt クライアント生成・接続
        self._mqtt = mqtt.Client(
            client_id=self.device_name,
            protocol=mqtt.MQTTv311,
            transport="tcp",
        )
        self._mqtt.username_pw_set(
            username=f"{iothub_hostname}/{self.device_name}/?api-version=2021-04-12",
            password=sas_token,
        )
        self._mqtt.tls_set(
            cert_reqs=ssl.CERT_REQUIRED,
            tls_version=ssl.PROTOCOL_TLS_CLIENT,
        )

        try:
            self._mqtt.connect(iothub_hostname, port=8883, keepalive=120)
            self._mqtt.loop_start()
            print(f"[MQTT] 接続完了 device={self.device_name} (device_id={self.device_id})")
        except Exception as e:
            raise RuntimeError(f"[MQTT] 接続失敗 {self.device_name}: {e}") from e

        # デバイス-to-クラウド メッセージトピック
        self._topic = f"devices/{self.device_name}/messages/events/"

    def on_stop(self):
        if hasattr(self, "_mqtt"):
            self._mqtt.loop_stop()
            self._mqtt.disconnect()
            print(f"[MQTT] 切断 device={self.device_name}")

    @task
    def send_telemetry(self):
        # 初回のみ: 起動後0〜5分のランダムなタイミングまで待機
        if self._first_send:
            time.sleep(self._initial_wait)
            self._first_send = False

        body = json.dumps(_make_telemetry(self.device_id)).encode("utf-8")

        start = time.perf_counter()
        try:
            self._mqtt.publish(self._topic, payload=body, qos=0)
            self._mqtt.loop(timeout=0.01)  # 送信キューを処理
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
#
# def _build_producer() -> Producer:
#     import re
#     conn_str = os.getenv("EVENTHUB_CONNECTION_STRING", "")
#     if not conn_str:
#         raise EnvironmentError("EVENTHUB_CONNECTION_STRING が設定されていません")
#     match = re.search(r"sb://([^.]+)\.servicebus\.windows\.net", conn_str)
#     if not match:
#         raise EnvironmentError(
#             "EVENTHUB_CONNECTION_STRING から名前空間を解析できませんでした"
#         )
#     namespace = match.group(1)
#     return Producer({
#         "bootstrap.servers": f"{namespace}.servicebus.windows.net:9093",
#         "security.protocol": "SASL_SSL",
#         "sasl.mechanism": "PLAIN",
#         "sasl.username": "$ConnectionString",
#         "sasl.password": conn_str,
#     })
#
#
# class KafkaDeviceUser(User):
#     """Event Hubs Kafka 互換エンドポイントへテレメトリを送信する（IoT Hub バイパス）。"""
#
#     wait_time = between(2, 5)
#
#     def on_start(self):
#         _registration_done.wait()
#         try:
#             self.device_id = _credential_queue.get_nowait()
#         except queue.Empty:
#             raise RuntimeError("デバイスキューが空です。")
#         self._topic = os.getenv("KAFKA_TOPIC") or os.getenv("EVENTHUB_NAME", "")
#         if not self._topic:
#             raise EnvironmentError("KAFKA_TOPIC または EVENTHUB_NAME が設定されていません")
#         self._producer = _build_producer()
#         print(f"[Kafka] 準備完了 device_id={self.device_id}")
#
#     def on_stop(self):
#         if hasattr(self, "_producer"):
#             self._producer.flush(timeout=5)
#
#     @task
#     def send_telemetry(self):
#         body = json.dumps(_make_telemetry(self.device_id)).encode("utf-8")
#         start = time.perf_counter()
#         try:
#             self._producer.produce(
#                 self._topic, key=str(self.device_id).encode(), value=body,
#             )
#             self._producer.poll(0)
#             elapsed_ms = int((time.perf_counter() - start) * 1000)
#             self.environment.events.request.fire(
#                 request_type="Kafka", name="send_telemetry",
#                 response_time=elapsed_ms, response_length=len(body),
#                 exception=None, context=self.context(),
#             )
#         except Exception as exc:
#             elapsed_ms = int((time.perf_counter() - start) * 1000)
#             self.environment.events.request.fire(
#                 request_type="Kafka", name="send_telemetry",
#                 response_time=elapsed_ms, response_length=0,
#                 exception=exc, context=self.context(),
#             )


# ---------------------------------------------------------------------------
# 旧シナリオ: ADLS Gen2 直接書き込み
# ---------------------------------------------------------------------------
# ※ 現在は silver_pipeline.py（Databricks）経由のフローを使用するためコメントアウト。
#    silver_pipeline が停止している場合の ADLS 疎通確認用として残す。
#    使用する場合は以下をアンコメントし、.env に ADLS 関連環境変数を設定すること。
#
# 本番フロー（通常使用）:
#   Locust（MqttDeviceUser）→ IoT Hub → Event Hub → silver_pipeline → ADLS
#
# 直接書き込みフロー（silver_pipeline 停止時の疎通確認）:
#   Locust（ADLSDeviceUser）→ ADLS（直接）
# ---------------------------------------------------------------------------

# import itertools as _itertools
# _adls_counter_lock = threading.Lock()
# _adls_device_counter = _itertools.count(int(os.getenv("DEVICE_ID_OFFSET", "1")))
#
# def _build_adls_client() -> DataLakeServiceClient:
#     tenant_id       = os.getenv("AZURE_TENANT_ID", "")
#     client_id_env   = os.getenv("AZURE_CLIENT_ID", "")
#     client_secret   = os.getenv("AZURE_CLIENT_SECRET", "")
#     storage_account = os.getenv("ADLS_STORAGE_ACCOUNT", "")
#     if not all([tenant_id, client_id_env, client_secret, storage_account]):
#         raise EnvironmentError(
#             "AZURE_TENANT_ID / AZURE_CLIENT_ID / AZURE_CLIENT_SECRET / "
#             "ADLS_STORAGE_ACCOUNT を設定してください"
#         )
#     credential = ClientSecretCredential(tenant_id, client_id_env, client_secret)
#     return DataLakeServiceClient(
#         account_url=f"https://{storage_account}.dfs.core.windows.net",
#         credential=credential,
#     )
#
# class ADLSDeviceUser(User):
#     """ADLS Gen2 へテレメトリ JSON を直接書き込む（silver_pipeline 停止時の疎通確認用）。"""
#     wait_time = between(2, 5)
#
#     def on_start(self):
#         with _adls_counter_lock:
#             self.device_id = next(_adls_device_counter)
#         self._container = os.getenv("ADLS_CONTAINER", "bronze")
#         self._prefix    = os.getenv("ADLS_PATH_PREFIX", "telemetry/loadtest")
#         self._client    = _build_adls_client()
#         self._fs        = self._client.get_file_system_client(self._container)
#         print(f"[ADLS] 準備完了 device_id={self.device_id}")
#
#     def on_stop(self):
#         if hasattr(self, "_client"):
#             self._client.close()
#
#     @task
#     def write_telemetry(self):
#         now       = datetime.now(timezone.utc)
#         date_path = now.strftime("%Y/%m/%d")
#         file_name = f"{now.strftime('%H%M%S%f')}_{uuid.uuid4().hex[:8]}.json"
#         path      = f"{self._prefix}/{self.device_id}/{date_path}/{file_name}"
#         body      = json.dumps(_make_telemetry(self.device_id)).encode("utf-8")
#         start     = time.perf_counter()
#         try:
#             self._fs.get_file_client(path).upload_data(body, overwrite=True)
#             elapsed_ms = int((time.perf_counter() - start) * 1000)
#             print(f"[ADLS] 書込み完了 device_id={self.device_id} path={path} {elapsed_ms}ms")
#             self.environment.events.request.fire(
#                 request_type="ADLS", name="write_telemetry",
#                 response_time=elapsed_ms, response_length=len(body),
#                 exception=None, context=self.context(),
#             )
#         except Exception as exc:
#             elapsed_ms = int((time.perf_counter() - start) * 1000)
#             print(f"[ADLS] 書込み失敗 device_id={self.device_id}: {exc}")
#             self.environment.events.request.fire(
#                 request_type="ADLS", name="write_telemetry",
#                 response_time=elapsed_ms, response_length=0,
#                 exception=exc, context=self.context(),
#             )


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

        props = self.consumer.get_eventhub_properties()
        self.partition_ids = props["partition_ids"]
        print(
            f"[EventHub] 接続成功: name={props['eventhub_name']}, "
            f"パーティション数={len(self.partition_ids)}"
        )

        self.last_seq = {}
        for pid in self.partition_ids:
            pp = self.consumer.get_partition_properties(pid)
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
                pp = self.consumer.get_partition_properties(pid)
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
