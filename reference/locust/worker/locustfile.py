import random
import sys
import ssl
import time
import argparse
from datetime import datetime, timedelta
import logging
import base64
import pandas as pd
from locust import User, task, TaskSet, events, constant, between, tag
from locust.env import Environment
from locust.runners import MasterRunner, WorkerRunner
from paho.mqtt import client as mqtt_client
from mqtt.client import MqttClient

# Logger設定
logger = logging.getLogger('user.adapter')
logger.setLevel(logging.INFO)
handler = logging.FileHandler('adapter_log.csv')
logger.addHandler(handler)
fmt = logging.Formatter(fmt='%(asctime)s.%(msecs)03d,%(name)s,%(levelname)s,%(message)s', datefmt="%Y/%m/%d %H:%M:%S")
handler.setFormatter(fmt)

adapter_list = []

# # メッセージ受信イベント（Worker）
def on_worker_setup_adapter(environment, msg, **kwargs):
    adapter_list.extend(msg.data)
    #for adapter in msg.data:
    #    logger.info(f"Received (User {adapter[0]} Host {adapter[2]})")
    environment.runner.send_message('res_setup_adapter', f"Received {len(msg.data)} adapter info!")

# メッセージ受信イベント（Master）
def on_master_recv_response(msg, **kwargs):
    logger.info(msg.data)

# 初期化イベント（Locust）
@events.init.add_listener
def on_locust_init(environment, **_kwargs):
    if isinstance(environment.runner, WorkerRunner):
        environment.runner.register_message('req_setup_adapter', on_worker_setup_adapter)
    if isinstance(environment.runner, MasterRunner):
        environment.runner.register_message('res_setup_adapter', on_master_recv_response)

# 開始イベント（Locust）
@events.test_start.add_listener
def on_test_start(environment, **_kwargs):
    if isinstance(environment.runner, MasterRunner):
        csv_file = pd.read_csv('adapter_list.csv').values.tolist()

        worker_count = environment.runner.worker_count
        chunk_size = int(len(csv_file) / worker_count)
        
        for i, worker in enumerate(environment.runner.clients):
            start_index = i * chunk_size

            if i + 1 < worker_count:
                end_index = start_index + chunk_size
            else:
                end_index = len(csv_file)

            data = csv_file[start_index:end_index]

            # workerへデータ送信
            environment.runner.send_message("req_setup_adapter", data, worker)

# 使うアダプタのリスト
adapter_list = pd.read_csv('adapter_list.csv').values.tolist()


class Adapter(User):
    # 待機秒数
    wait_time = constant(1)
    
    # コンストラクタ
    def __init__(self, environment: Environment):
        # スーパークラス初期化
        super().__init__(environment)

        # デバイス情報セット
        adapter_info = adapter_list.pop()
        logger.info(f"ctor Adapter {adapter_info[0]}")
        self.transport = "tcp"
        self.client_id = adapter_info[0]
        self.tls_context = ssl.create_default_context()
        self.host = adapter_info[2]
        self.type = adapter_info[3]
        self.port = 8883
        # シーケンス番号、メッセージIDの初期化
        self.seq_no = 0
        self.message_id = 0

        # MQTTクライアント生成
        self.client: MqttClient = MqttClient(
            environment=self.environment,
            transport=self.transport,
            client_id=self.client_id,
            protocol=mqtt_client.MQTTv311,
        )

        # 接続した際の処理。初期化、サブスクライブ
        # self.client.on_connect = self.on_connect
        # メッセージ受信時の処理。
        # self.client.on_message = self.on_message
        
        # ロングラン用のタイマー
        self.heartbeatTimer = datetime.now()
        
        # 定期通知は0分からオフセット秒を足した分秒で送信する
        self.regulerTimer = datetime.now().replace(minute=0, second=0, microsecond=0)
        offset = random.randint(0, 3559)
        self.regulerTimer += timedelta(seconds=offset)
        
        self.heartbeatInterval = timedelta(seconds=180)
        self.regulerInterval = timedelta(seconds=3600)
        
        if datetime.now() > self.regulerTimer:
            self.regulerTimer += self.regulerInterval

        # 初期化フラグ
        self.initflag = False
        self.isMaintenanced = False
        
        # IoTCore
        self.topic = adapter_info[0]
        certKey = 'key/' + adapter_info[0] + '-certificate.pem.crt'
        privateKey = 'key/' + adapter_info[0] + '-private.pem.key'
        rootKey = 'key/AmazonRootCA1.pem'

        self.client.tls_set(
            ca_certs=rootKey,
            certfile=certKey,
            keyfile=privateKey,
            cert_reqs=ssl.CERT_REQUIRED,
            tls_version=ssl.PROTOCOL_TLSv1_2,
            ciphers=None)
        self.client.tls_insecure_set(True)

        # ActiveMQ
        # self.username = adapter_info[0]
        # self.password = adapter_info[1]
        # # パラメータセット
        # self.client.tls_set_context(self.tls_context)
        # self.client.username_pw_set(username=self.username, password=self.password)

        # MQTTサーバ接続
        try:
            self.client.connect(host=self.host, port=self.port, keepalive=600)
            self.client.loop_start()
        except:
            logger.warning(f"connect failed. Adapter {adapter_info[0]}")
            adapter_list.append(adapter_info)

    
    # ロングランテスト
    @tag('longrun')
    @task
    def longRunMode(self):
        # メンテナンス済みに初期化
        if self.isMaintenanced == False:
            self.isMaintenanced = True
    
    # 平時のメッセージ送信。メンテナンス明け、ロングラン両方で利用
    @task
    @tag('longrun', 'maintenance')
    def generalyMessage(self):
        # メンテナンス済みでなければ終了
        if self.isMaintenanced == False:
            return

        # ハートビート通知と定期通知は固定間隔(3分と1時間)
        if datetime.now() > self.heartbeatTimer:
            self.ManagementMessage(1)
            self.heartbeatTimer = datetime.now() + self.heartbeatInterval
        if datetime.now() > self.regulerTimer:
            self.ManagementMessage(2)
            self.regulerTimer = datetime.now() + self.regulerInterval

    # メンテナンス明け一斉送信
    @tag('maintenance')
    @task
    def maintenanceMode(self):
        # メンテナンスは一度だけ実行
        if self.isMaintenanced == True:
            return
        
        self.ManagementMessage(1)
        self.isMaintenanced = True

    # 初期化処理
    def initializeConfig(self):
        if self.initflag == False:
            self.initflag = True
            self.ManagementMessage(1)

    # 送信処理
    def ManagementMessage(self, messageType):
        # 接続待ち
        count = 0
        while not self.client.connect_status:
            count += 1
            time.sleep(600)
            self.client.reconnect()
        
        result = False
        for i in range(20):

            # 90秒ごとに再送
            retry_time = datetime.now() + timedelta(seconds=90)
            # 15秒待機の処理×3セット。成功したら終わり
            for j in range(3):
                message = ''
                head = '/A'

                message = b'\x01'.hex()
                result = self.SendMessage(message, messageType, head)

                if result == True:
                    break
            if result == True:
                break

            # 90秒経過まで待機
            while datetime.now() <= retry_time:
                time.sleep(5)

        return result
    
    def SendMessage(self, message:str, messageID:int, head='/A'):
        # メッセージ送信。
        pub_topic = head + '/' + self.client_id + '/' + str(messageID)

        rc = self.client.publish(pub_topic, base64.b64encode(bytes.fromhex(message)).decode()) # type: ignore

        # 実行結果
        result = True   
        
        # メッセージ受信待ち。15秒でタイムアウト
        timeout_time = datetime.now() + timedelta(seconds=15)
        while rc == False:
            # スリープ
            time.sleep(1)
    
            # リトライアウト
            if timeout_time <= datetime.now():
                sub_topic = pub_topic.replace('/A/', '/T/')
                logger.info('%s,%s,%s,%s,%s,%s', self.client_id, "Response", "Timeout", sub_topic, str(self.message_id), "-")
                
                result = False
                break
        
        # シーケンス番号、メッセージIDをインクリメント
        self.seq_no += 1
        self.message_id += 1
        
        return result

    # イベントハンドラ（起動時）
    def on_start(self):
        time.sleep(5)

    # イベントハンドラ（停止時）
    def on_stop(self):
        # サーバ負荷分散
        if self.client.connect_status:
            # 0から10秒のランダムな秒数を生成
            wait_time = random.uniform(0, 10)

            # ランダムな秒数だけ待機
            time.sleep(wait_time)

            # 正常切断する
            self.client.disconnect()