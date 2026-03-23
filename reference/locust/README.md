# Locust

## アダプタ

### 開始方法

コンテナを起動する

a1uox8b02ehvtv-ats.iot.ap-northeast-1.amazonaws.com


#### IoTCore 向け

`adapter_list.csv` ファイルに、対象のアダプタを追加する。

```csv:adapter_list.csv
Thing,Password,Host,ConnectionType
"AD12512000","","a1cbptwh9b7aei-ats.iot.ap-northeast-1.amazonaws.com","IoTCore"
```

(1) ターミナルを二つ用意し、master-worker 構成で開始する。

```bash
# IoTCore 向けの機種 (K101) を指定する
locust --csv-full-history --master --tags longrun -u 1 -r 1 -t 60 --headless --expect-workers 1 --adapter "K101"
locust --worker --master-host=127.0.0.1 --adapter "K101"
```

(2) 一台構成で開始する。

```bash
# IoTCore 向けの機種 (K101) を指定する
locust --csv-full-history -u 1 -r 1 -t 60 --headless --adapter "K101"
```

## 画面 IF

### 開始方法

```bash
locust --csv-full-history -u 1 -r 1 -t 60 --headless
```