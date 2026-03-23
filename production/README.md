# IoTBricks 負荷検証 (Locust)

## 概要

Azure Event Hubs（Kafka 互換エンドポイント）に対して IoT デバイスの送信を模擬し、
`silver_pipeline.py`（Databricks）経由で Delta Lake（ADLS）への書き込みを検証する。

### データフロー

```
Locust（KafkaDeviceUser）
    │  Kafka send_telemetry（デバイス台数分）
    ▼
Azure Event Hubs（eh-telemetry）
    │
    ├─── [1] Event Hubs Capture（Azure ネイティブ機能・自動）
    │         ▼
    │    ADLS Bronze（RAW・Avro 形式）
    │
    └─── [2] Spark Structured Streaming（silver_pipeline.py）
              ▼
         ADLS Silver（Delta Lake: iot_catalog.silver.silver_sensor_data）
              +
         MySQL（OLTP）アラート状態・履歴更新
```

> **[1] Event Hubs Capture** は Azure Portal で設定する Azure ネイティブ機能。
> Locust や silver_pipeline のコードは不要。Event Hub にデータが届くと自動で ADLS Bronze へ Avro 形式で保存される。
>
> **[2] silver_pipeline.py**（Databricks LDP）は Event Hub を直接 Kafka で読み取り、
> 変換・アラート判定を行った上で ADLS Silver（Delta Lake）と MySQL に書き込む。

---

## 検証スコープと役割分担

### Locust で計測できる範囲

| 計測項目 | タスク名 | 出力先 |
|---|---|---|
| IoT Hub への MQTT 送信レイテンシ・成功率 | `MQTT / send_telemetry` | CSV・Web UI |
| Event Hub でのメッセージ到達確認（ポーリング） | `EventHub / check_throughput` | CSV・Web UI |
| 送信スループット（req/s）・失敗率 | 全タスク共通 | CSV・Web UI |

### Locust では計測できない範囲（Databricks 側で別途計測）

| 計測項目 | 計測方法 |
|---|---|
| silver_pipeline の処理遅延（E2E レイテンシ） | Databricks SQL（後述） |
| Delta Lake 書き込みスループット（件数/秒） | Databricks SQL（後述） |
| Event Hub 消費ラグ（処理追いつき確認） | Azure Portal → Event Hubs → メトリクス → Consumer Group Lag |
| Databricks バッチ処理時間 | Databricks UI → Pipelines → Pipeline Metrics |
| MySQL 書き込み遅延・接続数 | Azure Portal → MySQL → メトリクス |

---

## 推奨実行時間

| フェーズ | 構成 | 時間 | 目的 |
|---|---|---|---|
| 疎通確認 | single / double | 2〜5分 | 接続・送受信の動作確認 |
| **性能検証** | 20,000台（20 worker） | **30〜60分** | 安定したスループット・レイテンシを計測 |
| 安定性検証（ソークテスト） | 20,000台（20 worker） | 2〜4時間 | 長時間運用での劣化・接続断を確認 |

**30分を推奨する理由：**
- Spawn rate 200台/秒で 20,000台全員が起動するまで約100秒かかる
- その後、最低20分の安定稼働データが統計的に有意な平均値を得るために必要
- `foreachBatch`（10秒間隔）が 100回以上実行され、パイプラインの安定性が確認できる

---

## Azure リソースのスケール確認（実施前チェック）

20,000台規模の負荷検証前に以下を確認すること。

| 優先度 | リソース | 確認項目 | 確認場所 |
|---|---|---|---|
| **高** | IoT Hub | ティア（S3 推奨）・最大デバイス数 | Azure Portal → IoT Hub → 価格とスケール |
| **高** | Event Hubs | Throughput Units（8〜10 TU 以上）・パーティション数（8〜32） | Azure Portal → Event Hubs 名前空間 → スケール |
| **高** | MySQL Flexible Server | vCore 数（4以上）・最大接続数・IOPS | Azure Portal → MySQL → コンピューティング + ストレージ |
| **中** | Databricks Serverless | クォータ上限（DBU） | Databricks 管理コンソール → Account → Quotas |
| **低** | ADLS Gen2 | リクエストレート（20,000 req/秒・標準） | Azure Portal → ストレージアカウント → メトリクス |

---

## 構成ファイル

| ファイル | 用途 |
|---|---|
| `docker-compose.single.yml` | 1 デバイス疎通確認（スタンドアロン） |
| `docker-compose.double.yml` | 200 台疎通検証（worker × 2） |
| `docker-compose.yml` | 本番負荷検証 20,000 台（worker × 20） |
| `locustfile.py` | テストシナリオ本体 |
| `locust.conf` | Locust 既定設定（CSV 出力・ログ） |
| `Dockerfile` | Locust コンテナイメージ |
| `requirements.txt` | Python 依存パッケージ |
| `.env.example` | 環境変数テンプレート |
| `device.create.txt` | device_master 一括 INSERT SQL |

---

## 前提条件

- Docker / Docker Compose がインストール済みであること
- Azure Event Hubs の接続文字列を取得済みであること
- **Event Hubs Capture が Azure Portal で有効化されていること**
  - 有効であれば Locust がデータを送信すると自動的に ADLS Bronze へ Avro ファイルが保存される
- **silver_pipeline.py が Databricks 上で起動済みであること**
  - 起動していない場合、ADLS Silver および MySQL への書き込みは行われない
  - ADLS Bronze（Capture）への書き込みは silver_pipeline に依存しないため影響なし

---

## セットアップ

### 1. 環境変数ファイルの作成

```bash
cp .env.example .env
```

`.env` を編集して各値を設定する。

```env
# IoT Hub ホスト名（MqttDeviceUser の MQTT 接続先）
IOTHUB_HOSTNAME=iot-xxx.azure-devices.net

# Azure サービスプリンシパル（register_devices.py / EventHubConsumerUser 用）
# 必要ロール: IoT Hub Registry Contributor
AZURE_TENANT_ID=your-tenant-id
AZURE_CLIENT_ID=your-client-id
AZURE_CLIENT_SECRET=your-client-secret

# Event Hubs 接続文字列（EventHubConsumerUser のスループット確認用）
EVENTHUB_CONNECTION_STRING=Endpoint=sb://your-namespace.servicebus.windows.net/;...;EntityPath=eh-telemetry
EVENTHUB_NAME=eh-telemetry
```

### 2. IoT Hub へのデバイス登録（register_devices.py）

サービスプリンシパルを使用して IoT Hub にロードテスト用デバイスを一括登録し、
`device_credentials.csv`（参考資料の `adapter_list.csv` 相当）を生成する。

```bash
# 疎通検証用 200台（device_id 1〜200）
python register_devices.py --count 200 --offset 1

# 本番負荷検証用 20,000台（device_id 1〜20,000）
python register_devices.py --count 20000 --offset 1
```

- 既登録デバイスは自動スキップされるため、再実行しても安全
- 出力: `device_credentials.csv`（device_name, device_id, primary_key）
- docker-compose 起動時にコンテナへ自動マウントされる（読み取り専用）

### 3. device_master へのデバイス登録

負荷検証に使用するデバイス（device_id）を MySQL に登録する。
`device.create.txt` に記載の SQL を MySQL クライアントから実行する。

```
[A] 疎通検証用 200 台（docker-compose.double.yml 用）: device_id 3〜200
[B] 本番負荷検証用 20,000 台（docker-compose.yml 用）: device_id 201〜20,000
```

> **重要**: `silver_pipeline.py` は device_master に存在しない device_id のデータを
> MySQL へ書き込む際に FK エラーが発生するため、必ず事前に登録すること。

---

## 実行方法

### パターン 1: 単一コンテナ疎通確認（1 デバイス）

silver_pipeline の疎通確認として最初に実施する。

```bash
docker compose -f docker-compose.single.yml up --build
```

ブラウザで http://localhost:8089 を開き、以下を設定して **Start** を押す。

| パラメータ | 値 |
|---|---|
| Number of users | 2 |
| Spawn rate | 1 |
| Run time | 2m |

ヘッドレスで即時実行する場合:

```bash
docker compose -f docker-compose.single.yml run --rm \
  locust-single --headless -u 2 -r 1 --run-time 2m
```

### パターン 2: 2 worker 疎通検証（200 台）

```bash
docker compose -f docker-compose.double.yml up --build
```

ブラウザで http://localhost:8089 を開き、以下を設定して **Start** を押す。

| パラメータ | 値 |
|---|---|
| Number of users | 201 |
| Spawn rate | 10 |
| Run time | 5m |

ヘッドレスで即時実行する場合:

```bash
docker compose -f docker-compose.double.yml run --rm \
  master --headless -u 201 -r 10 --run-time 5m
```

### パターン 3: 本番負荷検証（20,000 台・20 worker）

```bash
docker compose up --build
```

ブラウザで http://localhost:8089 を開き、以下を設定して **Start** を押す。

| パラメータ | 値 |
|---|---|
| Number of users | 20001 |
| Spawn rate | 200 |
| Run time | 30m |

段階的な起動（worker を絞る場合）:

```bash
# まず worker-1〜2 のみで 2,000 台から確認する
docker compose up --build master worker-1 worker-2
```

停止:

```bash
docker compose down
docker compose -f docker-compose.double.yml down
docker compose -f docker-compose.single.yml down
```

---

## 統計出力（CSV）

参考資料の運用に倣い、`--csv-full-history` オプションで統計を CSV に出力する。
docker-compose.yml の master コンテナに設定済み。出力先は `./output/` ディレクトリ。

```
output/
├── locust_statistics.csv           # リクエスト統計（タスク別）
├── locust_statistics_history.csv   # 時系列統計（全履歴）
├── locust_statistics_failures.csv  # 失敗リスト
└── locust_statistics_exceptions.csv
```

コンテナ終了後に `output/` ディレクトリ内を確認する。

---

## ログ確認

### Locust ログ

コンテナのログは Docker 標準出力で確認する。

```bash
# master ログ
docker logs locust-master -f

# worker ログ
docker logs locust-double-worker-1 -f
```

### 書き込み確認

#### [1] ADLS Bronze（Event Hubs Capture）

Azure Portal または Storage Explorer で以下のパスに Avro ファイルが生成されていることを確認する。

```
bronze/
└── {eventhub-namespace}/
    └── {eventhub-name}/
        └── {partition}/
            └── {YYYY}/{MM}/{DD}/{HH}/{MM}/{SS}.avro
```

#### [2] ADLS Silver（silver_pipeline / Databricks LDP）

silver_pipeline が正常に処理しているか、Databricks 上で確認する。

```sql
-- 最新データ確認
SELECT device_id, event_timestamp, external_temp
FROM iot_catalog.silver.silver_sensor_data
ORDER BY event_timestamp DESC
LIMIT 50;

-- デバイス別受信件数
SELECT device_id, COUNT(*) AS cnt, MAX(event_timestamp) AS latest
FROM iot_catalog.silver.silver_sensor_data
GROUP BY device_id
ORDER BY device_id;
```

---

## 性能許容値

### E2E 処理遅延（目標値）

| 処理ステップ | 目標レイテンシ |
|---|---|
| Kafka → パイプライン | < 1秒 |
| JSON パース・変換 | < 500ms |
| アラート判定 | < 500ms |
| Delta Lake 書込み | < 2秒 |
| **End-to-End 合計** | **< 5秒** |

### パフォーマンスモニタリング閾値

| メトリクス | 閾値 | アクション |
|---|---|---|
| End-to-End レイテンシ | > 10秒 | クラスタスケールアップ |
| Kafka Consumer Lag | > 10,000 レコード | パーティション数・ワーカー数見直し |
| エラー率 | > 1% | 原因調査・アラート |

### スループット許容値

| テストシナリオ | データ量 | 期待結果（許容値） |
|---|---|---|
| 通常負荷 | 120件/秒（7,200件/分） | レイテンシ < 5秒 |
| ピーク負荷 | 700件/秒（42,000件/分） | レイテンシ < 10秒 |
| 障害復旧 | 1時間分のバックログ | 30分以内に追いつき |

---

## silver_pipeline 性能検証（Databricks 側）

Locust 実行中に Databricks から以下を定期実行して性能を計測する。

### ① E2E 処理遅延（イベント送信〜Delta Lake 書き込みまでの時間）

```sql
SELECT
    AVG(UNIX_TIMESTAMP(current_timestamp()) - UNIX_TIMESTAMP(event_timestamp)) AS avg_lag_sec,
    MAX(UNIX_TIMESTAMP(current_timestamp()) - UNIX_TIMESTAMP(event_timestamp)) AS max_lag_sec,
    COUNT(*) AS records_last_1min
FROM iot_catalog.silver.silver_sensor_data
WHERE event_timestamp >= current_timestamp() - INTERVAL 1 MINUTE;
```

### ② スループット（分ごとの処理件数）

```sql
SELECT
    DATE_TRUNC('minute', event_timestamp) AS minute,
    COUNT(*)              AS records,
    COUNT(DISTINCT device_id) AS devices
FROM iot_catalog.silver.silver_sensor_data
WHERE event_timestamp >= current_timestamp() - INTERVAL 10 MINUTE
GROUP BY 1
ORDER BY 1;
```

### ③ Event Hub 消費ラグ（パイプラインが追いついているか）

Azure Portal → Event Hubs 名前空間 → メトリクス → **Consumer Group Lag** を監視する。

- ラグが増え続ける → silver_pipeline の処理が送信速度に追いつけていない
- ラグが安定 / 減少 → 処理が追いついている

### ④ Databricks パイプラインメトリクス

Databricks UI → **Pipelines（Lakeflow）** → 対象パイプライン → **Pipeline Metrics** でバッチ処理時間・スループットを確認する。

---

## ADLS 直接書き込みシナリオについて（ADLSDeviceUser）

`locustfile.py` 内の `ADLSDeviceUser` クラスは現在コメントアウトされている。
これは silver_pipeline（Databricks）停止中でも ADLS への疎通確認を行うための
補助シナリオであり、**本番フローとは別経路**（Event Hub を経由しない）。

使用する場合は `locustfile.py` の該当箇所をアンコメントした上で、
`.env` に `ADLS_STORAGE_ACCOUNT` を追加して実行すること。

---

## クリーンアップ

### device_master の loadtest デバイスを削除する場合

```sql
-- device.create.txt の [C] セクションを参照
DELETE FROM device_master
WHERE device_name LIKE 'loadtest-device-%';
```
