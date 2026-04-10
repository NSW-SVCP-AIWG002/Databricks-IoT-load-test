# 負荷検証 運用コマンド集

> すべてのコマンドは **Master VM (10.10.1.4)** 上で実行する。

---

## VM 構成メモ

| VM | IP | docker バージョン | 起動方式 |
|---|---|---|---|
| locust-master | 10.10.1.4 | docker-compose v1 (1.29.2) | docker-compose |
| locust-worker-1 | 10.10.1.5 | docker-compose v1 (1.29.2) | **docker run** (v1バグ回避) |
| locust-worker-2 | 10.10.1.6 | docker-compose v1 (1.29.2) | **docker run** (v1バグ回避) |
| locust-worker-3 | 10.10.1.7 | docker-compose v1 (1.29.2) | **docker run** (v1バグ回避) |
| locust-worker-4 | 10.10.1.8 | docker-compose v1 (1.29.2) | **docker run** (v1バグ回避) |
| locust-worker-5 | 10.10.1.9 | docker compose v2 | docker compose |
| locust-worker-6 | 10.10.1.10 | docker compose v2 | docker compose |
| locust-worker-7 | 10.10.1.11 | docker compose v2 | docker compose |
| locust-worker-8 | 10.10.1.12 | docker compose v2 | docker compose |

> **VMs 1-4 の注意**: docker-compose 1.29.2 に `ContainerConfig` KeyError バグがあるため
> `docker-compose up` での再作成が失敗する。代わりに `docker run` で直接起動する。

---

## 1. 停止コマンド

### Master
```bash
cd ~/iotbricks/production && docker-compose down
```

### Workers VMs 1-4（SSH経由一括）
```bash
for ip in 10.10.1.5 10.10.1.6 10.10.1.7 10.10.1.8; do ssh azureuser@$ip "docker rm -f \$(docker ps -aq) 2>/dev/null && echo $ip done"; done
```

### Workers VMs 5-8（SSH経由一括）
```bash
for ip in 10.10.1.9 10.10.1.10 10.10.1.11 10.10.1.12; do ssh azureuser@$ip "cd ~/iotbricks/production && docker compose down && echo $ip done"; done
```

### 全台一括停止
```bash
cd ~/iotbricks/production && docker-compose down
for ip in 10.10.1.5 10.10.1.6 10.10.1.7 10.10.1.8; do ssh azureuser@$ip "docker rm -f \$(docker ps -aq) 2>/dev/null && echo $ip done"; done
for ip in 10.10.1.9 10.10.1.10 10.10.1.11 10.10.1.12; do ssh azureuser@$ip "cd ~/iotbricks/production && docker compose down && echo $ip done"; done
```

---

## 2. git 更新コマンド

locustfile.py や Dockerfile を変更した場合は、全VMへ配布・再ビルドが必要。

### Step 1: Master VM で git pull
```bash
git -C ~/iotbricks pull
```

### Step 2: 全 Worker VM へ locustfile.py を配布
```bash
for ip in 10.10.1.5 10.10.1.6 10.10.1.7 10.10.1.8 10.10.1.9 10.10.1.10 10.10.1.11 10.10.1.12; do scp ~/iotbricks/production/locustfile.py azureuser@$ip:~/iotbricks/production/locustfile.py && echo "$ip OK"; done
```

### Step 3: 全 Worker VM で古いイメージを削除して再ビルド
```bash
for ip in 10.10.1.5 10.10.1.6 10.10.1.7 10.10.1.8 10.10.1.9 10.10.1.10 10.10.1.11 10.10.1.12; do ssh azureuser@$ip "docker rmi iotbricks-locust:latest 2>/dev/null; cd ~/iotbricks/production && docker build --no-cache -t iotbricks-locust:latest . && echo $ip build OK"; done
```

> Dockerfile のみ変更した場合は Step 2 を省略できる。
> locustfile.py のみ変更した場合でも、イメージに焼き込まれているため Step 3 の再ビルドが必要。

---

## 3. 起動コマンド

### Step 1: Master 起動
```bash
cd ~/iotbricks/production && docker-compose up --build -d master
```

### Step 2: Workers VMs 1-4 起動（docker run）
```bash
for n in 1 2 3 4; do
  ip="10.10.1.$((n+4))"
  offset_a=$(( (n-1)*2600 + 1 ))
  offset_b=$(( (n-1)*2600 + 1301 ))
  ssh azureuser@$ip "cd ~/iotbricks/production && \
    docker run -d --name locust-worker-${n}a \
      --env-file .env \
      -e DEVICE_COUNT=1300 \
      -e DEVICE_ID_OFFSET=${offset_a} \
      -e MASTER_HOST=10.10.1.4 \
      -v ~/iotbricks/production/device_credentials.csv:/loadtest/device_credentials.csv:ro \
      --ulimit nofile=65536:65536 \
      iotbricks-locust:latest --worker --master-host=10.10.1.4 -f locustfile.py && \
    docker run -d --name locust-worker-${n}b \
      --env-file .env \
      -e DEVICE_COUNT=1300 \
      -e DEVICE_ID_OFFSET=${offset_b} \
      -e MASTER_HOST=10.10.1.4 \
      -v ~/iotbricks/production/device_credentials.csv:/loadtest/device_credentials.csv:ro \
      --ulimit nofile=65536:65536 \
      iotbricks-locust:latest --worker --master-host=10.10.1.4 -f locustfile.py && \
    echo VM${n} OK"
done
```

### Step 3: Workers VMs 5-8 起動（docker compose v2）
```bash
for n in 5 6 7 8; do
  ip="10.10.1.$((n+4))"
  ssh azureuser@$ip "cd ~/iotbricks/production && docker compose up -d worker-${n}a worker-${n}b && echo VM${n} OK"
done
```

---

## 4. 確認コマンド

### コンテナ起動状態の確認
```bash
for ip in 10.10.1.5 10.10.1.6 10.10.1.7 10.10.1.8 10.10.1.9 10.10.1.10 10.10.1.11 10.10.1.12; do echo "=== $ip ===" && ssh azureuser@$ip "docker ps --format 'table {{.Names}}\t{{.Status}}'"; done
```

### イメージに正しい locustfile.py が焼き込まれているか確認
```bash
for ip in 10.10.1.5 10.10.1.6 10.10.1.7 10.10.1.8 10.10.1.9 10.10.1.10 10.10.1.11 10.10.1.12; do echo "=== $ip ===" && ssh azureuser@$ip "docker run --rm --entrypoint grep iotbricks-locust:latest -n 'get_eventhub_properties\|EVENTHUB_PARTITION_COUNT' locustfile.py"; done
```

> 期待値: `EVENTHUB_PARTITION_COUNT` のみ表示され、`get_eventhub_properties` が出ないこと

### Worker ログ確認（エラー確認）
```bash
# VMs 1-4
for n in 1 2 3 4; do ip="10.10.1.$((n+4))"; echo "=== VM${n} worker-${n}a ===" && ssh azureuser@$ip "docker logs locust-worker-${n}a 2>&1 | tail -20"; done

# VMs 5-8
for n in 5 6 7 8; do ip="10.10.1.$((n+4))"; echo "=== VM${n} worker-${n}a ===" && ssh azureuser@$ip "docker logs locust-worker-${n}a 2>&1 | tail -20"; done
```

---

## 5. テスト開始

Locust Web UI: **http://10.10.1.4:8089**

| 項目 | 値 |
|---|---|
| Number of users | 20801 |
| Spawn rate | 20 |
| Host | .env の TARGET_HOST の値 |

Workers 欄に **16台** 表示されていることを確認してからテスト開始。

---

## 6. デバイス ID 割り当て一覧

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
