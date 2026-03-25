# 負荷検証 観点表

## 性能許容値

### E2E 処理遅延

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

## ① Locust 計測項目

### 事前確認

| # | 確認項目 | 確認方法 | 期待値 |
|---|---|---|---|
| 1 | device_credentials.csv が存在する | `ls production/device_credentials.csv` | ファイルが存在する |
| 2 | IoT Hub デバイスが登録済み | Azure Portal → IoT Hub → デバイス | 対象台数分のデバイスが存在する |
| 3 | MySQL device_master に対象デバイスが登録済み | `SELECT COUNT(*) FROM device_master WHERE device_name LIKE 'loadtest-device-%'` | 対象台数分のレコードが存在する |
| 4 | .env の設定値が正しい | `cat production/.env` | IOTHUB_HOSTNAME・EVENTHUB_CONNECTION_STRING が設定されている |
| 5 | Databricks Job（silver_pipeline）が起動済み | Databricks UI → Workflows → Jobs | ステータスが Running |

---

### 送信性能（MQTT / send_telemetry）

| # | 確認項目 | 確認方法 | 期待値 |
|---|---|---|---|
| 6 | IoT Hub への送信成功率 | Locust Web UI → Statistics → MQTT / send_telemetry の # Fails | 0件（失敗率 0%） |
| 7 | 送信レイテンシ（中央値） | Locust Web UI → Statistics → Median (ms) | 許容範囲内であること |
| 8 | 送信レイテンシ（95パーセンタイル） | Locust Web UI → Statistics → 95%ile (ms) | 許容範囲内であること |
| 9 | 送信スループット | Locust Web UI → Statistics → Current RPS | 目標 req/s を満たしていること |
| 10 | 全デバイスが起動完了している | Locust Web UI → Workers または ログ | `All users spawned` が出力されている |

---

### Event Hub 到達確認（EventHub / check_throughput）

| # | 確認項目 | 確認方法 | 期待値 |
|---|---|---|---|
| 11 | Event Hub へのメッセージ到達 | Locust Web UI → Statistics → EventHub / check_throughput の response_length | 新着件数が継続的に増加している |
| 12 | Event Hub 確認の失敗率 | Locust Web UI → Statistics → # Fails | 0件（失敗率 0%） |

---

### 結果出力確認

| # | 確認項目 | 確認方法 | 期待値 |
|---|---|---|---|
| 13 | CSV ファイルが出力されている | `ls production/output/` | locust_statistics*.csv が存在する |
| 14 | 失敗リストに記録がないこと | `production/output/locust_statistics_failures.csv` | 空またはヘッダーのみ |

---

## ② パイプライン起動後の確認項目

### Azure Event Hubs（Azure Portal）

| # | 確認項目 | 確認方法 | 期待値 |
|---|---|---|---|
| 15 | Event Hub への受信メッセージ数 | Azure Portal → Event Hubs 名前空間 → メトリクス → 受信メッセージ | 送信台数に応じたメッセージが増加している |
| 16 | Consumer Group Lag（処理追いつき確認） | Azure Portal → Event Hubs → メトリクス → Consumer Group Lag | ラグが増加し続けていない（安定または減少） |
| 17 | Throughput Units の上限に達していないこと | Azure Portal → Event Hubs 名前空間 → メトリクス → スロットルエラー | スロットルエラーが発生していない |
| 18 | Incoming / Outgoing Messages が一定していること | Azure Portal → Event Hubs 名前空間 → **概要** → Incoming Messages・Outgoing Messages のグラフを確認 | 負荷検証中、両値が一定の範囲内で安定して推移している（急激な増減・断絶がない） |

---

### ADLS Bronze（Event Hubs Capture）

| # | 確認項目 | 確認方法 | 期待値 |
|---|---|---|---|
| 19 | Avro ファイルが生成されている | Azure Portal → ストレージアカウント → コンテナ（bronze） | `{namespace}/{eventhub}/{partition}/{YYYY}/{MM}/{DD}/` 配下に .avro ファイルが存在する |
| 20 | ファイルが定期的に作成されている | 上記パスのタイムスタンプを確認 | Capture の時間ウィンドウ（5分）ごとにファイルが追加されている |
| 21 | IoT Hub 送信〜Avro ファイル生成までの遅延（目検） | ① Locust Web UI → Statistics → `MQTT / send_telemetry` の最終送信時刻を記録<br>② Azure Portal → ストレージアカウント → コンテナ（bronze） → 最新 .avro ファイルの「最終更新日時」を確認<br>③ ②－① の差分を計算 | Capture の時間ウィンドウ（5分）＋バッファを考慮し、概ね **10分以内** にファイルが生成されている |

---

### ADLS Silver / Unity Catalog（Databricks SQL）

| # | 確認項目 | 確認方法 | 期待値 |
|---|---|---|---|
| 22 | silver_sensor_data にデータが書き込まれている | `SELECT COUNT(*) FROM iot_catalog.silver.silver_sensor_data` | レコードが増加している |
| 23 | 最新データのタイムスタンプが直近である | `SELECT device_id, event_timestamp FROM iot_catalog.silver.silver_sensor_data ORDER BY event_timestamp DESC LIMIT 10` | event_timestamp が現在時刻に近い |
| 24 | E2E 処理遅延が許容範囲内である | `SELECT AVG(UNIX_TIMESTAMP(current_timestamp()) - UNIX_TIMESTAMP(event_timestamp)) AS avg_lag_sec FROM iot_catalog.silver.silver_sensor_data WHERE event_timestamp >= current_timestamp() - INTERVAL 1 MINUTE` | avg_lag_sec < 5（警戒ライン: 10秒超でアクション） |
| 25 | 分ごとのスループットが安定している | `SELECT DATE_TRUNC('minute', event_timestamp) AS minute, COUNT(*) AS records FROM iot_catalog.silver.silver_sensor_data WHERE event_timestamp >= current_timestamp() - INTERVAL 10 MINUTE GROUP BY 1 ORDER BY 1` | 通常: 7,200件/分以上、ピーク: 42,000件/分 |
| 26 | 全デバイスのデータが届いている | `SELECT COUNT(DISTINCT device_id) FROM iot_catalog.silver.silver_sensor_data WHERE event_timestamp >= current_timestamp() - INTERVAL 10 MINUTE` | 送信台数と一致している |

---

### MySQL（Azure Portal / SQL クライアント）

| # | 確認項目 | 確認方法 | 期待値 |
|---|---|---|---|
| 27 | silver_sensor_data にデータが書き込まれている | `SELECT COUNT(*) FROM silver_sensor_data` | レコードが増加している |
| 28 | E2E 処理遅延が許容範囲内である | `SELECT AVG(UNIX_TIMESTAMP(NOW()) - UNIX_TIMESTAMP(event_timestamp)) AS avg_lag_sec, MAX(UNIX_TIMESTAMP(NOW()) - UNIX_TIMESTAMP(event_timestamp)) AS max_lag_sec, COUNT(*) AS records_last_1min FROM silver_sensor_data WHERE event_timestamp >= NOW() - INTERVAL 1 MINUTE` | avg_lag_sec < 5（警戒ライン: 10秒超でアクション） |
| 29 | MySQL の接続数が上限に達していないこと | Azure Portal → MySQL → メトリクス → アクティブな接続数 | 最大接続数の上限に達していない |
| 30 | MySQL の IOPS が上限に達していないこと | Azure Portal → MySQL → メトリクス → IO % | 100% に張り付いていない |

---

### Databricks（Databricks UI）

| # | 確認項目 | 確認方法 | 期待値 |
|---|---|---|---|
| 31 | silver_pipeline の Job が正常実行されている | Databricks UI → Workflows → Jobs → 対象 Job | ステータスがエラーなく実行されている |
| 32 | バッチ処理時間が許容範囲内である | Databricks UI → Jobs → Pipeline Metrics | 各バッチの処理時間が安定している |
| 33 | エラーログが出ていないこと | Databricks UI → Jobs → Run Output | `[SILVER_ERR_*]` のエラーログが出ていない |
| 34 | silver_pipeline 取込時のワーカー数を確認 | `SELECT COUNT(DISTINCT instance_id) AS worker_count, DATE_TRUNC('minute', start_time) AS minute FROM unitycatalog.compute.node_timeline WHERE start_time >= current_timestamp() - INTERVAL 10 MINUTE GROUP BY 2 ORDER BY 2` | 取込処理中の分ごとのワーカー（インスタンス）数を記録する |
