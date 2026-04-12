# 負荷検証 運用コマンド集

> すべてのコマンドは **Master VM (10.10.1.4)** 上で実行する。

---

## VM 構成メモ

| VM | IP | docker バージョン | 起動方式 |
|---|---|---|---|
| locust-master | 10.10.1.4 | docker compose v2 | docker compose |
| locust-worker-1 | 10.10.1.5 | docker compose v2.24.7 | docker compose |
| locust-worker-2 | 10.10.1.6 | docker compose v2.24.7 | docker compose |
| locust-worker-3 | 10.10.1.7 | docker compose v2.24.7 | docker compose |
| locust-worker-4 | 10.10.1.8 | docker compose v2.24.7 | docker compose |
| locust-worker-5 | 10.10.1.9 | docker compose v2.24.7 | docker compose |
| locust-worker-6 | 10.10.1.10 | docker compose v2.24.7 | docker compose |
| locust-worker-7 | 10.10.1.11 | docker compose v2.24.7 | docker compose |
| locust-worker-8 | 10.10.1.12 | docker compose v2.24.7 | docker compose |

> 全 VM docker compose v2 で統一。

---

## 0. Master VM docker compose v2 インストール（初回のみ）

Master VM (10.10.1.4) が docker-compose v1 の場合は以下でv2へ更新する：

```bash
sudo apt-get update && sudo apt-get install -y docker-compose-plugin && docker compose version
```

インストール確認：`Docker Compose version v2.x.x` と表示されれば完了。

---

## 1. ログリセット＆Master再起動

ログを消去してMasterをクリーンに再起動する場合（再検証時など）：

```bash
cd ~/iotbricks/production && docker rm -f locust-master 2>/dev/null; docker ps -a | awk '{print $1}' | tail -n +2 | xargs docker rm -f 2>/dev/null; sudo truncate -s 0 output/locust.log; docker compose up --build -d master
```

---

## 2. 停止コマンド

### Master
```bash
cd ~/iotbricks/production && docker compose down
```

### Workers VMs 1-8（SSH経由一括）
```bash
for ip in 10.10.1.5 10.10.1.6 10.10.1.7 10.10.1.8 10.10.1.9 10.10.1.10 10.10.1.11 10.10.1.12; do ssh azureuser@$ip "cd ~/iotbricks/production && docker compose down && echo $ip done"; done
```

### 全台一括停止
```bash
cd ~/iotbricks/production && docker compose down && for ip in 10.10.1.5 10.10.1.6 10.10.1.7 10.10.1.8 10.10.1.9 10.10.1.10 10.10.1.11 10.10.1.12; do ssh azureuser@$ip "cd ~/iotbricks/production && docker compose down && echo $ip done"; done
```

---

## 2. .env 更新コマンド

### EVENTHUB_CONNECTION_STRING を更新する場合

```bash
# Master VM の .env を更新（接続文字列とパーティション数を変更）
sed -i 's|EVENTHUB_CONNECTION_STRING=.*|EVENTHUB_CONNECTION_STRING=Endpoint=sb://ehns-nsw-iotbricks-dev.servicebus.windows.net/;SharedAccessKeyName=locust-consumer;SharedAccessKey=<YOUR_SHARED_ACCESS_KEY>;EntityPath=eh-telemetry|' ~/iotbricks/production/.env && sed -i 's|EVENTHUB_PARTITION_COUNT=.*|EVENTHUB_PARTITION_COUNT=2|' ~/iotbricks/production/.env

# 更新確認
grep -E "EVENTHUB_CONNECTION_STRING|EVENTHUB_PARTITION_COUNT" ~/iotbricks/production/.env

# 全 Worker VM に配布
for ip in 10.10.1.5 10.10.1.6 10.10.1.7 10.10.1.8 10.10.1.9 10.10.1.10 10.10.1.11 10.10.1.12; do scp ~/iotbricks/production/.env azureuser@$ip:~/iotbricks/production/.env && echo "$ip OK"; done
```

> 接続文字列を変更する場合は上記の `SharedAccessKey=` と `EntityPath=` の値を新しい値に置き換えること。

---

## 3. git 更新コマンド

locustfile.py を変更した場合は配布のみで反映される（ボリュームマウントのため再ビルド不要）。
Dockerfile / requirements.txt を変更した場合のみ再ビルドが必要。

### locustfile.py のみ変更した場合

#### Step 1: Master VM で git pull
```bash
git -C ~/iotbricks pull
```

#### Step 2: 全 Worker VM へ locustfile.py を配布
```bash
for ip in 10.10.1.5 10.10.1.6 10.10.1.7 10.10.1.8 10.10.1.9 10.10.1.10 10.10.1.11 10.10.1.12; do scp ~/iotbricks/production/locustfile.py azureuser@$ip:~/iotbricks/production/locustfile.py && echo "$ip OK"; done
```

> 配布後、ワーカーを再起動すれば新しい locustfile.py が反映される。

### Dockerfile / requirements.txt を変更した場合（再ビルドが必要）

#### Step 1: Master VM の Dockerfile を更新（uamqp アンインストール追加）
```bash
sed -i 's|RUN pip install --no-cache-dir -r requirements.txt|RUN pip install --no-cache-dir -r requirements.txt \&\& pip uninstall -y uamqp 2>/dev/null \|\| true|' ~/iotbricks/production/Dockerfile && grep uamqp ~/iotbricks/production/Dockerfile
```

#### Step 2: 全 Worker VM へ Dockerfile を配布
```bash
for ip in 10.10.1.5 10.10.1.6 10.10.1.7 10.10.1.8 10.10.1.9 10.10.1.10 10.10.1.11 10.10.1.12; do scp ~/iotbricks/production/Dockerfile azureuser@$ip:~/iotbricks/production/Dockerfile && echo "$ip OK"; done
```

#### Step 3: 全 Worker VM で再ビルド
```bash
for ip in 10.10.1.5 10.10.1.6 10.10.1.7 10.10.1.8 10.10.1.9 10.10.1.10 10.10.1.11 10.10.1.12; do ssh azureuser@$ip "docker rmi iotbricks-locust:latest 2>/dev/null; cd ~/iotbricks/production && docker build --no-cache -t iotbricks-locust:latest . && echo $ip build OK"; done
```

---

## 4. 起動コマンド

### Step 1: Master 起動
```bash
cd ~/iotbricks/production && docker compose up --build -d master
```

### Step 2: Workers VMs 1-8 起動（docker run 統一）

> locustfile.py はボリュームマウントで反映されるため再ビルド不要。

```bash
for i in 1 2 3 4 5 6 7 8; do ip="10.10.1.$((4+i))"; offset_a=$(( (i-1)*2600 + 1 )); offset_b=$(( (i-1)*2600 + 1301 )); ssh azureuser@$ip "docker stop locust-worker-${i}a locust-worker-${i}b 2>/dev/null; docker rm locust-worker-${i}a locust-worker-${i}b 2>/dev/null; docker run -d --name locust-worker-${i}a --env-file ~/iotbricks/production/.env -e DEVICE_COUNT=1300 -e DEVICE_ID_OFFSET=$offset_a -v ~/iotbricks/production/device_credentials.csv:/loadtest/device_credentials.csv:ro -v ~/iotbricks/production/locustfile.py:/loadtest/locustfile.py:ro --ulimit nofile=100000:100000 iotbricks-locust:latest --worker --master-host=10.10.1.4 -f locustfile.py; docker run -d --name locust-worker-${i}b --env-file ~/iotbricks/production/.env -e DEVICE_COUNT=1300 -e DEVICE_ID_OFFSET=$offset_b -v ~/iotbricks/production/device_credentials.csv:/loadtest/device_credentials.csv:ro -v ~/iotbricks/production/locustfile.py:/loadtest/locustfile.py:ro --ulimit nofile=100000:100000 iotbricks-locust:latest --worker --master-host=10.10.1.4 -f locustfile.py"; done
```

---

## 5. 確認コマンド

### コンテナ起動状態の確認
```bash
for ip in 10.10.1.5 10.10.1.6 10.10.1.7 10.10.1.8 10.10.1.9 10.10.1.10 10.10.1.11 10.10.1.12; do echo "=== $ip ===" && ssh azureuser@$ip "docker ps --format 'table {{.Names}}\t{{.Status}}'"; done
```

### イメージに正しい locustfile.py が焼き込まれているか確認
```bash
for ip in 10.10.1.5 10.10.1.6 10.10.1.7 10.10.1.8 10.10.1.9 10.10.1.10 10.10.1.11 10.10.1.12; do echo "=== $ip ===" && ssh azureuser@$ip "docker run --rm --entrypoint grep iotbricks-locust:latest -n 'get_eventhub_properties\|EVENTHUB_PARTITION_COUNT' locustfile.py"; done
```

### Worker ログ確認（エラー確認）
```bash
for n in 1 2 3 4 5 6 7 8; do ip="10.10.1.$((n+4))"; echo "=== VM${n} worker-${n}a ===" && ssh azureuser@$ip "docker logs locust-worker-${n}a 2>&1 | tail -20"; done
```

### EventHub get_partition_properties 動作確認（診断用、Worker VM上で直接実行）
```bash
docker exec locust-worker-1a python3 -c "from azure.eventhub import EventHubConsumerClient; c=EventHubConsumerClient.from_connection_string('Endpoint=sb://ehns-nsw-iotbricks-dev.servicebus.windows.net/;SharedAccessKeyName=locust-consumer;SharedAccessKey=<YOUR_SHARED_ACCESS_KEY>;EntityPath=eh-telemetry', consumer_group='\$Default'); props=c.get_eventhub_properties(); print('partitions:', props['partition_ids']); [print(pid, c.get_partition_properties(pid)['last_enqueued_sequence_number']) for pid in props['partition_ids']]; c.close(); print('OK')"
```

---

## 6. テスト開始

Locust Web UI: **http://10.10.1.4:8089**

| 項目 | 値 |
|---|---|
| Number of users | 20801 |
| Spawn rate | 20 |
| Host | .env の TARGET_HOST の値 |

Workers 欄に **16台** 表示されていることを確認してからテスト開始。

---

## 7. デバイス ID 割り当て一覧

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
