# 負荷検証環境 再構築ドキュメント

> 作成日: 2026-04-17
> 目的: 負荷をかける側（Locust VM群）を一時削除後に完全再構築するための手順書

---

## 1. システム全体構成

### データフロー

```
Locust（MqttDeviceUser × 20,800台）
    │  MQTT over TLS（ポート 8883）
    ▼
Azure IoT Hub（iot-nsw-iotbricks-dev）
    │  メッセージルーティング
    ▼
Azure Event Hub Namespace（ehns-nsw-iotbricks-dev）
    └── Event Hub: eh-telemetry（パーティション数: 16）
         │
         ├─ [1] Event Hubs Capture（自動）
         │       → ADLS Gen2 Bronze（Avro形式）
         │
         └─ [2] Databricks Structured Streaming（silver_pipeline.py）
                 → ADLS Gen2 Silver（Delta Lake: iot_catalog.silver.silver_sensor_data）
                 → MySQL（OLTP: アラート状態・履歴・デバイスステータス）
```

### 役割分担

| コンポーネント | 役割 | 削除対象 |
|---|---|---|
| Locust VM × 9台 | 負荷生成（MQTT送信）| **YES（今回削除）** |
| Azure IoT Hub | デバイス認証・メッセージ受信 | No |
| Azure Event Hub | メッセージキュー | No |
| Databricks | ストリーム処理・書き込み | No |
| MySQL | OLTP更新 | No |
| ADLS Gen2 | データ保存（Bronze/Silver） | No |

---

## 2. Azure リソース一覧

### IoT Hub

| 項目 | 値 |
|---|---|
| リソース名 | `iot-nsw-iotbricks-dev` |
| ホスト名 | `iot-nsw-iotbricks-dev.azure-devices.net` |
| 登録デバイス数 | 20,800台（loadtest-device-1 〜 loadtest-device-20800） |
| 認証方式 | SAS トークン（対称鍵）|
| 必要ロール | IoT Hub Registry Contributor |

### Event Hub Namespace

| 項目 | 値 |
|---|---|
| 名前空間名 | `ehns-nsw-iotbricks-dev` |
| Event Hub 名 | `eh-telemetry` |
| パーティション数 | **16** |
| コンシューマーグループ | `$Default` のみ |
| Capture | 有効（ADLS Bronze へ Avro 出力） |
| アクセスポリシー | `locust-consumer`（Listen 権限） |
| 接続文字列形式 | `Endpoint=sb://ehns-nsw-iotbricks-dev.servicebus.windows.net/;SharedAccessKeyName=locust-consumer;SharedAccessKey=<KEY>;EntityPath=eh-telemetry` |

### MySQL

| 項目 | 値 |
|---|---|
| リソース名 | `mysql-nsw-iotbricks-dev` |
| データベース名 | `iot_app_db` |
| 最大接続数 | 余裕あり（テスト時最大 8本） |

### Databricks

| 項目 | 値 |
|---|---|
| チェックポイント場所 | `abfss://bronze@<STORAGE_ACCOUNT>.dfs.core.windows.net/_checkpoints/silver_pipeline/` |
| シルバーテーブル | `iot_catalog.silver.silver_sensor_data` |
| Spark トリガー間隔 | 10秒 |
| kafka.startingOffsets | `latest`（チェックポイント削除後に設定済） |

### サービスプリンシパル（register_devices.py 用）

| 項目 | 値 |
|---|---|
| AZURE_TENANT_ID | `.env.example` 参照 |
| AZURE_CLIENT_ID | `.env.example` 参照 |
| AZURE_CLIENT_SECRET | `.env.example` 参照（要再確認・期限切れの可能性あり） |
| 必要ロール | IoT Hub Registry Contributor |

---

## 3. Locust VM 構成

### VM 一覧

| VM名 | プライベート IP | 役割 | 起動コンテナ |
|---|---|---|---|
| locust-master | 10.10.1.4 | Web UI / ワーカー統括 | master × 1 |
| locust-worker-1 | 10.10.1.5 | 負荷生成 | worker-1a, worker-1b |
| locust-worker-2 | 10.10.1.6 | 負荷生成 | worker-2a, worker-2b |
| locust-worker-3 | 10.10.1.7 | 負荷生成 | worker-3a, worker-3b |
| locust-worker-4 | 10.10.1.8 | 負荷生成 | worker-4a, worker-4b |
| locust-worker-5 | 10.10.1.9 | 負荷生成 | worker-5a, worker-5b |
| locust-worker-6 | 10.10.1.10 | 負荷生成 | worker-6a, worker-6b |
| locust-worker-7 | 10.10.1.11 | 負荷生成 | worker-7a, worker-7b |
| locust-worker-8 | 10.10.1.12 | 負荷生成 | worker-8a, worker-8b |

**合計: VM 9台、コンテナ 17台（master × 1 + worker × 16）**

### VM スペック要件

| 項目 | Master | Worker |
|---|---|---|
| CPU | 8コア以上 | 2コア以上（各コンテナ 1コア使用） |
| OS | Ubuntu（azureuser） | Ubuntu（azureuser） |
| Docker | Compose v2 必須 | Compose v2 必須 |
| ネットワーク | 同一 VNet・プライベート IP | 同一 VNet・プライベート IP |
| ポート開放 | 8089（Web UI）, 5557（Worker通信） | 5557（Master通信）|

### デバイス ID 割り当て

| Worker | VM IP | DEVICE_ID_OFFSET | デバイス範囲 |
|---|---|---|---|
| worker-1a | 10.10.1.5 | 1 | 1〜1300 |
| worker-1b | 10.10.1.5 | 1301 | 1301〜2600 |
| worker-2a | 10.10.1.6 | 2601 | 2601〜3900 |
| worker-2b | 10.10.1.6 | 3901 | 3901〜5200 |
| worker-3a | 10.10.1.7 | 5201 | 5201〜6500 |
| worker-3b | 10.10.1.7 | 6501 | 6501〜7800 |
| worker-4a | 10.10.1.8 | 7801 | 7801〜9100 |
| worker-4b | 10.10.1.8 | 9101 | 9101〜10400 |
| worker-5a | 10.10.1.9 | 10401 | 10401〜11700 |
| worker-5b | 10.10.1.9 | 11701 | 11701〜13000 |
| worker-6a | 10.10.1.10 | 13001 | 13001〜14300 |
| worker-6b | 10.10.1.10 | 14301 | 14301〜15600 |
| worker-7a | 10.10.1.11 | 15601 | 15601〜16900 |
| worker-7b | 10.10.1.11 | 16901 | 16901〜18200 |
| worker-8a | 10.10.1.12 | 18201 | 18201〜19500 |
| worker-8b | 10.10.1.12 | 19501 | 19501〜20800 |

---

## 4. ファイル構成

```
production/
├── locustfile.py               # テストシナリオ本体（MQTT送信・EventHub監視）
├── register_devices.py         # IoT Hub デバイス一括登録スクリプト
├── device_credentials.csv      # 生成済みデバイス認証情報（device_name, device_id, primary_key）
├── device.create.txt           # MySQL device_master 一括 INSERT SQL
├── Dockerfile                  # Locust コンテナイメージ（Python 3.11-slim）
├── requirements.txt            # Python 依存パッケージ
├── docker-compose.yml          # 本番構成（20,800台・worker × 16）
├── docker-compose.double.yml   # 疎通確認（200台・worker × 2）
├── docker-compose.single.yml   # 1台疎通確認
├── locust.conf                 # Locust 設定（Web UI・CSV出力）
├── .env                        # 環境変数（実値・git 管理外）
├── .env.example                # 環境変数テンプレート
├── cloud-init-worker.yml       # VM 作成時の自動セットアップスクリプト
├── commands.md                 # 運用コマンド集
└── output/                     # CSV・ログ出力ディレクトリ
```

### 主要設定値（locustfile.py）

| 設定 | 値 | 説明 |
|---|---|---|
| wait_time | between(270, 330) | 送信間隔（平均5分） |
| keepalive | 100秒 | MQTT keepalive |
| SAS トークン有効期限 | 86,400秒（24時間） | テスト毎に自動生成 |
| QoS | 0 | Fire-and-forget |
| プロトコル | MQTTv3.1.1 over TLS（ポート8883） | |
| 初回送信分散 | random.uniform(0, 300) | 初回送信をランダム分散 |
| EventHubConsumerUser | fixed_count=1 | 監視用・1台のみ |

---

## 5. 環境変数一覧

`.env` ファイルに設定する値（`.env.example` をコピーして作成）。

| 変数名 | 説明 | 取得元 |
|---|---|---|
| `TARGET_HOST` | Locust Web UI 表示用（`http://localhost`） | 固定値 |
| `IOTHUB_HOSTNAME` | IoT Hub ホスト名 | Azure Portal → IoT Hub → 概要 |
| `AZURE_TENANT_ID` | サービスプリンシパル テナント ID | Entra ID → アプリ登録 |
| `AZURE_CLIENT_ID` | サービスプリンシパル クライアント ID | Entra ID → アプリ登録 |
| `AZURE_CLIENT_SECRET` | サービスプリンシパル シークレット | **期限切れ確認要**・Entra ID → 証明書とシークレット |
| `EVENTHUB_CONNECTION_STRING` | Event Hub 接続文字列（EntityPath 含む） | Azure Portal → Event Hub → 共有アクセスポリシー |
| `EVENTHUB_NAME` | `eh-telemetry` | 固定値 |
| `EVENTHUB_PARTITION_COUNT` | `16` | Event Hub のパーティション数 |

---

## 6. 再構築手順

### STEP 1: Azure VM の作成（Worker × 8台 + Master × 1台）

#### 1-1. VM スペック

| 項目 | 推奨値 |
|---|---|
| イメージ | Ubuntu 22.04 LTS |
| サイズ | Master: 8 vCPU以上 / Worker: 2 vCPU以上 |
| 認証 | SSH公開鍵（azureuser） |
| VNet | 既存 VNet に参加（IoT Hub・Event Hub と同一ネットワーク） |
| プライベート IP | 静的割り当て（10.10.1.4〜10.10.1.12） |
| カスタムデータ | `cloud-init-worker.yml` の内容を貼り付け（Worker VM のみ） |

#### 1-2. Master VM セットアップ（手動）

```bash
# Docker インストール
sudo apt-get update
sudo apt-get install -y docker.io docker-compose-plugin
sudo systemctl enable docker
sudo systemctl start docker
sudo usermod -aG docker azureuser
# 一度ログアウト・再ログイン後に確認
docker compose version  # v2.x.x であること

# リポジトリ取得
cd ~
git clone <リポジトリURL> iotbricks
cd iotbricks/production

# 環境変数設定
cp .env.example .env
nano .env  # 各値を設定

# device_credentials.csv の確認
ls -la device_credentials.csv  # 存在すること
wc -l device_credentials.csv   # 20801行（ヘッダー含む）であること
```

#### 1-3. Worker VM セットアップ（cloud-init 自動 or 手動）

`cloud-init-worker.yml` を Azure VM 作成時の「カスタムデータ」に貼り付けると自動セットアップされる。
手動の場合は Master と同手順で Docker をインストール後、以下を実行：

```bash
# リポジトリ取得
cd ~
git clone <リポジトリURL> iotbricks

# Master から .env を配布（Master VM から実行）
for ip in 10.10.1.5 10.10.1.6 10.10.1.7 10.10.1.8 10.10.1.9 10.10.1.10 10.10.1.11 10.10.1.12; do
  scp ~/iotbricks/production/.env azureuser@$ip:~/iotbricks/production/.env && echo "$ip OK"
done

# Master から device_credentials.csv を配布（Master VM から実行）
for ip in 10.10.1.5 10.10.1.6 10.10.1.7 10.10.1.8 10.10.1.9 10.10.1.10 10.10.1.11 10.10.1.12; do
  scp ~/iotbricks/production/device_credentials.csv azureuser@$ip:~/iotbricks/production/device_credentials.csv && echo "$ip OK"
done
```

---

### STEP 2: IoT Hub デバイス登録

IoT Hub にデバイスが登録されていない場合は以下を実行（既登録なら不要）。

```bash
# Master VM で実行
cd ~/iotbricks/production

# Python 仮想環境セットアップ（初回のみ）
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# デバイス登録（20,800台）
python register_devices.py --count 20800 --offset 1
# → device_credentials.csv が生成される（20,801行：ヘッダー + 20,800台）
# → 既登録デバイスは自動スキップされるため再実行安全
```

---

### STEP 3: MySQL device_master 登録

MySQL に負荷検証デバイスが未登録の場合のみ実施（既登録なら不要）。
`device.create.txt` 内の INSERT SQL を MySQL クライアントから実行：

```sql
-- device_master に loadtest-device-1 〜 loadtest-device-20800 を INSERT
-- device.create.txt の [A][B] セクション参照
```

> **注意**: device_master に存在しない device_id のデータを silver_pipeline が処理すると FK エラーが発生する。

---

### STEP 4: Docker イメージのビルド

#### Master VM

```bash
cd ~/iotbricks/production
docker compose up --build -d master
```

#### Worker VM 1〜8（Master VM から SSH 一括実行）

```bash
# イメージビルド（各 Worker VM で実行）
for ip in 10.10.1.5 10.10.1.6 10.10.1.7 10.10.1.8 10.10.1.9 10.10.1.10 10.10.1.11 10.10.1.12; do
  ssh azureuser@$ip "cd ~/iotbricks/production && docker build --no-cache -t iotbricks-locust:latest . && echo $ip build OK"
done
```

---

### STEP 5: Databricks silver_pipeline の起動確認

テスト開始前に Databricks Job（silver_pipeline.py）が起動済みであることを確認する。

- Databricks UI → Jobs → silver_pipeline → **Running** であること
- 起動していない場合: Job を手動起動

**重要設定の確認（silver_pipeline.py）：**

```python
# kafka_options の startingOffsets（現在の設定）
"startingOffsets": "latest",  # チェックポイント削除後はこのまま
```

---

### STEP 6: Worker コンテナ起動

Master VM から SSH 経由で Worker コンテナを一括起動：

```bash
for i in 1 2 3 4 5 6 7 8; do
  ip="10.10.1.$((4+i))"
  offset_a=$(( (i-1)*2600 + 1 ))
  offset_b=$(( (i-1)*2600 + 1301 ))
  ssh azureuser@$ip "
    docker stop locust-worker-${i}a locust-worker-${i}b 2>/dev/null
    docker rm locust-worker-${i}a locust-worker-${i}b 2>/dev/null
    docker run -d --name locust-worker-${i}a \
      --env-file ~/iotbricks/production/.env \
      -e DEVICE_COUNT=1300 \
      -e DEVICE_ID_OFFSET=$offset_a \
      -v ~/iotbricks/production/device_credentials.csv:/loadtest/device_credentials.csv:ro \
      -v ~/iotbricks/production/locustfile.py:/loadtest/locustfile.py:ro \
      --ulimit nofile=100000:100000 \
      iotbricks-locust:latest \
      --worker --master-host=10.10.1.4 -f locustfile.py
    docker run -d --name locust-worker-${i}b \
      --env-file ~/iotbricks/production/.env \
      -e DEVICE_COUNT=1300 \
      -e DEVICE_ID_OFFSET=$offset_b \
      -v ~/iotbricks/production/device_credentials.csv:/loadtest/device_credentials.csv:ro \
      -v ~/iotbricks/production/locustfile.py:/loadtest/locustfile.py:ro \
      --ulimit nofile=100000:100000 \
      iotbricks-locust:latest \
      --worker --master-host=10.10.1.4 -f locustfile.py
  "
done
```

---

### STEP 7: 起動確認

```bash
# Worker 接続状態確認（Master の Web UI か下記コマンドで確認）
for ip in 10.10.1.5 10.10.1.6 10.10.1.7 10.10.1.8 10.10.1.9 10.10.1.10 10.10.1.11 10.10.1.12; do
  echo "=== $ip ===" && ssh azureuser@$ip "docker ps --format 'table {{.Names}}\t{{.Status}}'"
done

# Worker ログ確認（エラーがないこと）
for n in 1 2 3 4 5 6 7 8; do
  ip="10.10.1.$((n+4))"
  echo "=== VM${n} worker-${n}a ===" && ssh azureuser@$ip "docker logs locust-worker-${n}a 2>&1 | tail -20"
done
```

---

### STEP 8: テスト開始

Locust Web UI: **http://10.10.1.4:8089**

| パラメータ | 値 |
|---|---|
| Number of users | **20,801**（MqttDeviceUser×20,800 + EventHubConsumerUser×1） |
| Spawn rate | **20**（users/秒） |
| Host | `.env` の TARGET_HOST の値 |

Workers 欄に **16台** 表示されていることを確認してから Start。

---

## 7. テスト実行・確認

### テスト実行設定

| 項目 | 値 |
|---|---|
| 実行時間 | 60分 |
| 全台接続完了時間 | 約 17分（20,800台 ÷ 20 users/s = 1,040秒） |
| 定常状態 RPS（理論値） | 20,800 ÷ 300秒 ≈ **69.3 req/s** |

### 停止コマンド

```bash
# 全台一括停止（Master VM から実行）
cd ~/iotbricks/production && docker compose down && \
for ip in 10.10.1.5 10.10.1.6 10.10.1.7 10.10.1.8 10.10.1.9 10.10.1.10 10.10.1.11 10.10.1.12; do
  ssh azureuser@$ip "cd ~/iotbricks/production && docker compose down && echo $ip done"
done
```

### パイプライン確認 SQL（Databricks）

```sql
-- E2E 遅延確認
SELECT
    AVG(UNIX_TIMESTAMP(create_time) - UNIX_TIMESTAMP(event_timestamp)) AS avg_lag_sec,
    MAX(UNIX_TIMESTAMP(create_time) - UNIX_TIMESTAMP(event_timestamp)) AS max_lag_sec,
    COUNT(*) AS records_last_1min
FROM iot_catalog.silver.silver_sensor_data
WHERE event_timestamp >= current_timestamp() - INTERVAL 1 MINUTE;

-- 分スループット確認
SELECT
    DATE_TRUNC('minute', event_timestamp) AS minute,
    COUNT(*) AS records,
    COUNT(DISTINCT device_id) AS devices
FROM iot_catalog.silver.silver_sensor_data
WHERE event_timestamp >= current_timestamp() - INTERVAL 10 MINUTE
GROUP BY 1
ORDER BY 1;
```

---

## 8. locustfile.py 配布（コード変更時）

```bash
# Master で git pull
git -C ~/iotbricks pull

# 全 Worker に配布
for ip in 10.10.1.5 10.10.1.6 10.10.1.7 10.10.1.8 10.10.1.9 10.10.1.10 10.10.1.11 10.10.1.12; do
  scp ~/iotbricks/production/locustfile.py azureuser@$ip:~/iotbricks/production/locustfile.py && echo "$ip OK"
done
# ボリュームマウントのため Worker 再起動だけで反映（再ビルド不要）
```

---

## 9. これまでの検証結果サマリー

| Run | 日時（JST） | 送信間隔 | keepalive | デバイス数 | 定常 RPS | E2E遅延(avg) | 備考 |
|---|---|---|---|---|---|---|---|
| 1 | - | 90秒 | - | - | - | - | 初回疎通 |
| 2 | - | 90秒 | - | 20,500 | 228 req/s | avg 221秒 | EventHub スロットリング 192k件 |
| 3 | - | - | - | - | - | - | - |
| 4 | - | 5分 | 100秒 | 20,500 | - | - | disconnect 0件確認 |
| 5 | 2026-04-14 11:52〜12:52 | 5分 | 100秒 | 20,500 | 68.3 req/s | avg 23秒 | スロットリング 0・全台100% |

### Run 5 詳細（最新・参照ベース）

| 指標 | 値 |
|---|---|
| 総送信数 | 184,203件（失敗0件） |
| silver_sensor_data | 180,600件（MySQL・Databricks一致） |
| カバレッジ | 20,500台 100% |
| E2E遅延（Databricks avg） | 23秒 |
| E2E遅延（MySQL avg） | 33秒 |
| Databricks ワーカー数 | 2台（全期間固定） |
| EventHub スロットリング | 0件 |

---

## 10. 既知の課題・注意事項

| 項目 | 内容 | 対応状況 |
|---|---|---|
| Outgoing >> Incoming | Event Hub Capture が旧バックログを消化するため Outgoing が Incoming の数倍になる | 調査中（データ品質への影響なし） |
| E2E 遅延 23秒 | Databricks マイクロバッチ処理間隔（トリガー10秒）が主因。目標 < 5秒に未達 | トリガー間隔短縮を今後検討 |
| SAS トークン有効期限 | `AZURE_CLIENT_SECRET` は期限切れの可能性あり。再構築前に確認・再発行を推奨 | 再構築時に要確認 |
| startingOffsets | チェックポイント削除後に `"latest"` に変更済み。再構築時はこの設定のまま使用 | 変更済み |
| silver_pipeline ログ | 全ログに `datetime.now()` の日時を追加済み | 対応済み |

---

## 11. 送信間隔別 コード設定一覧

locustfile.py の送信間隔を切り替える際に変更が必要な箇所をまとめる。

### 設定値比較

| 設定箇所 | 90秒間隔（Run 1・2） | 5分間隔（Run 4・5・現在） |
|---|---|---|
| `wait_time` | `between(60, 120)` | `between(270, 330)` |
| `keepalive`（秒） | `600` | `100` |
| `on_start` 初回送信分散 | `random.uniform(0, 60)` | `random.uniform(0, 300)` |
| 定常 RPS（20,800台時） | 約 231 req/s | 約 69 req/s |
| EventHub スロットリング | **発生（192k件）** | なし |

### 90秒間隔 変更箇所（locustfile.py 抜粋）

```python
class MqttDeviceUser(User):

    # ① 送信間隔: 60〜120秒（平均90秒）
    wait_time = between(60, 120)

    def __init__(self, environment):
        ...
        # ② keepalive: 600秒（wait_time max 120秒より十分長い値）
        self._mqtt.connect(iothub_hostname, port=8883, keepalive=600)
        ...

    def on_start(self):
        ...
        # ③ 初回送信分散: 0〜60秒（wait_time 下限 60秒に合わせる）
        time.sleep(random.uniform(0, 60))
```

### 5分間隔 変更箇所（locustfile.py 抜粋・現在の設定）

```python
class MqttDeviceUser(User):

    # ① 送信間隔: 270〜330秒（平均5分）
    wait_time = between(270, 330)

    def __init__(self, environment):
        ...
        # ② keepalive: 100秒（5分待機中に複数回 PINGREQ を送る）
        self._mqtt.connect(iothub_hostname, port=8883, keepalive=100)
        ...

    def on_start(self):
        ...
        # ③ 初回送信分散: 0〜300秒（wait_time 全周期に分散）
        time.sleep(random.uniform(0, 300))
```

### keepalive の考え方

| wait_time | keepalive | 理由 |
|---|---|---|
| between(60, 120) | 600秒 | wait_time max（120秒）の5倍以上。接続維持に余裕を持たせる |
| between(270, 330) | 100秒 | 5分待機中に複数回（約3回）PINGREQ を送り、ブローカー側の切断を防ぐ |

> **注意**: keepalive が wait_time より短い場合（例: keepalive=100、wait_time=between(270,330)）、
> gevent スケジューラの遅延で PINGREQ が送れず disconnect（rc=16）が発生することがある。
> Run 3（7分間隔・keepalive=300）で disconnect 多発した経験あり。

### Run 2（90秒間隔）実績値

| 指標 | 値 |
|---|---|
| 日時（JST） | 2026-04-13 01:45〜02:35 |
| 実行時間 | 約 50分 |
| デバイス数 | 20,500台 |
| Spawn rate | 10 users/s |
| 総送信数 | 453,461件（失敗 0件） |
| 定常 RPS | 225〜230 req/s |
| E2E 遅延（avg・Databricks） | 221秒（約 3.7分） |
| E2E 遅延（max・Databricks） | 726秒（約 12.1分） |
| EventHub Throttled Requests | **192,490件** ← 主な課題 |
| silver_sensor_data | 453,373件（MySQL・Databricks一致、100%カバレッジ） |

---

## 12. チェックポイントリセット手順

次回 Run 前にバックログを持ち込まないようにするため、チェックポイントをリセットする。

```python
# Databricks ノートブックで実行（silver_pipeline 停止後に実施）
STORAGE_ACCOUNT = dbutils.secrets.get("storage_secrets", "storage-account-name")
checkpoint_path = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/_checkpoints/silver_pipeline/"
dbutils.fs.rm(checkpoint_path, recurse=True)
print("チェックポイント削除完了")
```

**実施順序**:
1. Databricks Job（silver_pipeline）を停止
2. 上記コードを実行してチェックポイント削除
3. silver_pipeline.py の `startingOffsets: "latest"` を確認（現在設定済み）
4. Databricks Job を再起動
