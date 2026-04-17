"""
シルバー層LDPパイプライン

Kafkaからテレメトリデータを取得し、シルバー層Delta Lakeに書き込む。
アラート判定・OLTP DB更新・メール送信キュー登録を foreachBatch で実行する。

実行環境: Databricks（spark / dbutils は Databricks ランタイムが提供）
"""

import builtins
builtins.dbutils = dbutils  # noqa: F821
builtins.spark = spark      # noqa: F821

from functions.mysql_connector import get_mysql_connection

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType,
)

from functions.alert_judgment import (
    check_alerts_with_duration,
    enqueue_email_notification,
    evaluate_threshold,
    get_alert_abnormal_state,
    insert_alert_history,
    update_alert_abnormal_state,
    update_alert_history_on_recovery,
    update_device_status,
)
from functions.constants import PIPELINE_TRIGGER_INTERVAL, TOPIC_NAME
from functions.device_id_extraction import (
    extract_device_id_udf,
)
from functions.json_telemetry import convert_to_json_with_device_id_udf

print("[INIT] インポート完了")

# =============================================================================
# 接続設定（Databricks Secrets から取得）
# =============================================================================

EVENTHUBS_NAMESPACE = dbutils.secrets.get("eventhubs_secrets", "eventhubs-namespace")
EVENTHUBS_CONNECTION_STRING = dbutils.secrets.get("eventhubs_secrets", "eventhubs-connection-string")

HOST = dbutils.secrets.get("my_sql_secrets", "host")
DATABASE = "iot_app_db"
PORT = 3306
OLTP_JDBC_URL = f"jdbc:mysql://{HOST}:{PORT}/{DATABASE}?useSSL=true&requireSSL=true&verifyServerCertificate=true"

STORAGE_ACCOUNT = dbutils.secrets.get("storage_secrets", "storage-account-name")
CHECKPOINT_LOCATION = (
    f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/_checkpoints/silver_pipeline/"
)

SILVER_TABLE = "iot_catalog.silver.silver_sensor_data"

print("[INIT] 接続設定完了")

# =============================================================================
# Spark Connect foreachBatch 互換: シークレットの事前解決
# =============================================================================
# foreachBatch ワーカーでは dbutils / DBUtils が利用不可なため、
# ドライバーでシークレットを事前解決し、シリアライズ可能なプロキシに格納する。
# プロキシは secrets.get(scope, key) インターフェースを維持するため、
# 他ファイルの builtins.dbutils.secrets.get() 呼び出しはそのまま動作する。

class _SecretsCache:
    """シリアライズ可能な secrets.get() キャッシュ"""
    def __init__(self):
        self._cache = {}
    def get(self, scope, key):
        return self._cache[(scope, key)]

class _DBUtilsProxy:
    """シリアライズ可能な dbutils プロキシ（foreachBatch ワーカー用）"""
    def __init__(self):
        self.secrets = _SecretsCache()

_dbutils_proxy = _DBUtilsProxy()
for _key in ["host", "username", "password"]:
    _dbutils_proxy.secrets._cache[("my_sql_secrets", _key)] = (
        dbutils.secrets.get("my_sql_secrets", _key)
    )
builtins.dbutils = _dbutils_proxy

print("[INIT] シークレットプロキシ設定完了")

# =============================================================================
# Kafkaオプション
# =============================================================================

kafka_options = {
    "kafka.bootstrap.servers": f"{EVENTHUBS_NAMESPACE}.servicebus.windows.net:9093",
    "subscribe": TOPIC_NAME,
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="$ConnectionString" '
        f'password="{EVENTHUBS_CONNECTION_STRING}";'
    ),
    # チェックポイントなし初回起動時は earliest から開始（チェックポイントがあれば自動でそこから再開）
    "startingOffsets": "earliest",
    # "startingOffsets": "latest",
    "failOnDataLoss": "false",
    "kafka.session.timeout.ms": "30000",       # 30秒（デフォルト600秒を短縮）
    "kafka.heartbeat.interval.ms": "10000",    # 10秒（session.timeout の1/3以下）
    "kafka.request.timeout.ms": "60000",       # 60秒
    "kafka.max.poll.interval.ms": "600000",    # 10分（foreachBatch の処理時間上限）
}

# =============================================================================
# センサーデータスキーマ
# =============================================================================

sensor_schema = StructType([
    StructField("device_id", IntegerType(), False),
    StructField("event_timestamp", StringType(), False),
    StructField("external_temp", DoubleType(), True),
    StructField("set_temp_freezer_1", DoubleType(), True),
    StructField("internal_sensor_temp_freezer_1", DoubleType(), True),
    StructField("internal_temp_freezer_1", DoubleType(), True),
    StructField("df_temp_freezer_1", DoubleType(), True),
    StructField("condensing_temp_freezer_1", DoubleType(), True),
    StructField("adjusted_internal_temp_freezer_1", DoubleType(), True),
    StructField("set_temp_freezer_2", DoubleType(), True),
    StructField("internal_sensor_temp_freezer_2", DoubleType(), True),
    StructField("internal_temp_freezer_2", DoubleType(), True),
    StructField("df_temp_freezer_2", DoubleType(), True),
    StructField("condensing_temp_freezer_2", DoubleType(), True),
    StructField("adjusted_internal_temp_freezer_2", DoubleType(), True),
    StructField("compressor_freezer_1", DoubleType(), True),
    StructField("compressor_freezer_2", DoubleType(), True),
    StructField("fan_motor_1", DoubleType(), True),
    StructField("fan_motor_2", DoubleType(), True),
    StructField("fan_motor_3", DoubleType(), True),
    StructField("fan_motor_4", DoubleType(), True),
    StructField("fan_motor_5", DoubleType(), True),
    StructField("defrost_heater_output_1", DoubleType(), True),
    StructField("defrost_heater_output_2", DoubleType(), True),
])


# =============================================================================
# マスタデータ取得（Spark JDBC）
# =============================================================================

def _jdbc_options(table: str) -> dict:
    """JDBC共通オプションを返す"""
    return {
        "url": OLTP_JDBC_URL,
        # "driver": "com.mysql.cj.jdbc.Driver",
        "dbtable": table,
        "user": builtins.dbutils.secrets.get("my_sql_secrets", "username"),
        "password": builtins.dbutils.secrets.get("my_sql_secrets", "password"),
    }


def get_device_master():
    """デバイスマスタを取得（Unity Catalog）"""
    return spark.table("iot_catalog.oltp_db.device_master")


def get_organization_master():
    """組織マスタを取得（Unity Catalog）"""
    return spark.table("iot_catalog.oltp_db.organization_master")


def get_alert_settings():
    """アラート設定マスタを取得"""
    return (
        builtins.spark.read.format("jdbc").options(**_jdbc_options("alert_setting_master")).load()
        .filter("delete_flag = FALSE")
    )


# =============================================================================
# foreachBatchコールバック
# =============================================================================

def process_sensor_batch(batch_df, batch_id):
    """
    マイクロバッチ処理メイン（foreachBatch コールバック）

    処理フロー:
    1. 空バッチスキップ
    2. マスタデータ取得
    3. 閾値判定・継続時間判定
    4. Delta Lake書込み
    5. OLTP更新（異常状態/アラート履歴/メール送信キュー/デバイスステータス）
    """
    print(f"[BATCH {batch_id}] ===== foreachBatch 開始 =====")

    # Spark Connect: バッチDFからSparkSessionを取得し builtins を更新
    builtins.spark = batch_df.sparkSession
    # シークレット事前解決済みプロキシをワーカーの builtins に設定
    builtins.dbutils = _dbutils_proxy

    record_count = batch_df.count()
    print(f"[BATCH {batch_id}] レコード数={record_count}")

    if batch_df.isEmpty():
        print(f"[BATCH {batch_id}] 空のためスキップ")
        return

    # マスタデータ取得（バッチごとに最新を参照）
    print(f"[BATCH {batch_id}] アラート設定マスタ取得開始")
    alert_settings_df = get_alert_settings()
    print(f"[BATCH {batch_id}] アラート設定マスタ取得完了: {alert_settings_df.count()}件")

    print(f"[BATCH {batch_id}] アラート異常状態取得開始")
    alert_state_df = get_alert_abnormal_state()
    print(f"[BATCH {batch_id}] アラート異常状態取得完了")

    # STEP 4: アラート判定（閾値 + 継続時間）
    print(f"[BATCH {batch_id}] STEP4: 閾値判定開始")
    threshold_df = evaluate_threshold(batch_df, alert_settings_df)
    print(f"[BATCH {batch_id}] STEP4: 閾値判定完了")

    print(f"[BATCH {batch_id}] STEP4: 継続時間判定開始")
    alert_df = check_alerts_with_duration(threshold_df, alert_state_df)
    print(f"[BATCH {batch_id}] STEP4: 継続時間判定完了")

    # STEP 5a: Delta Lake書込み（センサーデータ）
    print(f"[BATCH {batch_id}] STEP5a: Delta Lake 書込み開始 → {SILVER_TABLE}")
    output_df = alert_df.select(
        F.col("device_id"),
        F.col("organization_id"),
        F.col("event_timestamp"),
        F.to_date(F.col("event_timestamp")).alias("event_date"),
        *[F.col(f) for f in [
            "external_temp", "set_temp_freezer_1",
            "internal_sensor_temp_freezer_1", "internal_temp_freezer_1",
            "df_temp_freezer_1", "condensing_temp_freezer_1",
            "adjusted_internal_temp_freezer_1", "set_temp_freezer_2",
            "internal_sensor_temp_freezer_2", "internal_temp_freezer_2",
            "df_temp_freezer_2", "condensing_temp_freezer_2",
            "adjusted_internal_temp_freezer_2", "compressor_freezer_1",
            "compressor_freezer_2", "fan_motor_1", "fan_motor_2",
            "fan_motor_3", "fan_motor_4", "fan_motor_5",
            "defrost_heater_output_1", "defrost_heater_output_2",
        ]],
        # 疎通検証
        # F.col("raw_json").alias("sensor_data_json"),
        F.expr("parse_json(raw_json)").alias("sensor_data_json"),
        F.current_timestamp().alias("create_time"),
    )

    output_df.write.format("delta").mode("append").saveAsTable(SILVER_TABLE)
    print(f"[BATCH {batch_id}] STEP5a: Delta Lake 書込み完了")

    # STEP 5a-2: MySQL書込み（センサーデータ）
    print(f"[BATCH {batch_id}] STEP5a-2: MySQL用 DataFrame 生成開始")
    mysql_df = batch_df.select(
        F.col("device_id"),
        F.col("organization_id"),
        F.col("event_timestamp"),
        F.to_date(F.col("event_timestamp")).alias("event_date"),
        *[F.col(f) for f in [
            "external_temp", "set_temp_freezer_1",
            "internal_sensor_temp_freezer_1", "internal_temp_freezer_1",
            "df_temp_freezer_1", "condensing_temp_freezer_1",
            "adjusted_internal_temp_freezer_1", "set_temp_freezer_2",
            "internal_sensor_temp_freezer_2", "internal_temp_freezer_2",
            "df_temp_freezer_2", "condensing_temp_freezer_2",
            "adjusted_internal_temp_freezer_2", "compressor_freezer_1",
            "compressor_freezer_2", "fan_motor_1", "fan_motor_2",
            "fan_motor_3", "fan_motor_4", "fan_motor_5",
            "defrost_heater_output_1", "defrost_heater_output_2",
        ]],
    )
    print(f"[BATCH {batch_id}] STEP5a-2: MySQL用 DataFrame 生成完了")

    try:
        print(f"[BATCH {batch_id}] STEP5a-2: mysql_df.collect() 開始")
        mysql_records = mysql_df.collect()
        print(f"[BATCH {batch_id}] STEP5a-2: mysql_df.collect() 完了 → {len(mysql_records)}件")

        if mysql_records:
            print(f"[BATCH {batch_id}] STEP5a-2: MySQL接続開始")
            with get_mysql_connection() as conn:
                print(f"[BATCH {batch_id}] STEP5a-2: MySQL接続成功")
                with conn.cursor() as cursor:
                    print(f"[BATCH {batch_id}] STEP5a-2: INSERT 実行開始")
                    cursor.executemany("""
                        INSERT INTO silver_sensor_data (
                            device_id, organization_id, event_timestamp, event_date,
                            external_temp, set_temp_freezer_1,
                            internal_sensor_temp_freezer_1, internal_temp_freezer_1,
                            df_temp_freezer_1, condensing_temp_freezer_1,
                            adjusted_internal_temp_freezer_1, set_temp_freezer_2,
                            internal_sensor_temp_freezer_2, internal_temp_freezer_2,
                            df_temp_freezer_2, condensing_temp_freezer_2,
                            adjusted_internal_temp_freezer_2, compressor_freezer_1,
                            compressor_freezer_2, fan_motor_1, fan_motor_2,
                            fan_motor_3, fan_motor_4, fan_motor_5,
                            defrost_heater_output_1, defrost_heater_output_2,
                            create_time
                        ) VALUES (
                            %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, NOW(6)
                        )
                    """, [
                        (
                            r["device_id"], r["organization_id"],
                            r["event_timestamp"], r["event_date"],
                            r["external_temp"], r["set_temp_freezer_1"],
                            r["internal_sensor_temp_freezer_1"], r["internal_temp_freezer_1"],
                            r["df_temp_freezer_1"], r["condensing_temp_freezer_1"],
                            r["adjusted_internal_temp_freezer_1"], r["set_temp_freezer_2"],
                            r["internal_sensor_temp_freezer_2"], r["internal_temp_freezer_2"],
                            r["df_temp_freezer_2"], r["condensing_temp_freezer_2"],
                            r["adjusted_internal_temp_freezer_2"], r["compressor_freezer_1"],
                            r["compressor_freezer_2"], r["fan_motor_1"], r["fan_motor_2"],
                            r["fan_motor_3"], r["fan_motor_4"], r["fan_motor_5"],
                            r["defrost_heater_output_1"], r["defrost_heater_output_2"],
                        )
                        for r in mysql_records
                    ])
                    print(f"[BATCH {batch_id}] STEP5a-2: INSERT 実行完了")
                conn.commit()
                print(f"[BATCH {batch_id}] STEP5a-2: COMMIT 完了")
            print(f"[BATCH {batch_id}] STEP5a-2: MySQL silver_sensor_data 書込み成功: {len(mysql_records)}行")
        else:
            print(f"[BATCH {batch_id}] STEP5a-2: mysql_records が空のためスキップ")
    except Exception as e:
        print(f"[SILVER_ERR_007] MySQLへのセンサーデータ書込みに失敗しました: {e}")
        import traceback
        print(traceback.format_exc())

    # STEP 5b: OLTP更新
    print(f"[BATCH {batch_id}] STEP5b: アラート異常状態 更新開始")
    try:
        update_alert_abnormal_state(alert_df, batch_id)
        print(f"[BATCH {batch_id}] STEP5b: アラート異常状態 更新完了")
    except Exception as e:
        print(f"[SILVER_ERR_006] アラート異常状態への書込みに失敗しました: {e}")
        import traceback
        print(traceback.format_exc())

    print(f"[BATCH {batch_id}] STEP5b: アラート履歴 登録開始")
    try:
        insert_alert_history(alert_df, batch_id)
        print(f"[BATCH {batch_id}] STEP5b: アラート履歴 登録完了")
    except Exception as e:
        print(f"[SILVER_ERR_004] アラート履歴の登録に失敗しました: {e}")
        import traceback
        print(traceback.format_exc())

    print(f"[BATCH {batch_id}] STEP5b: アラート履歴 復旧更新開始")
    try:
        update_alert_history_on_recovery(alert_df, batch_id)
        print(f"[BATCH {batch_id}] STEP5b: アラート履歴 復旧更新完了")
    except Exception as e:
        print(f"[SILVER_ERR_004] アラート履歴の復旧更新に失敗しました: {e}")
        import traceback
        print(traceback.format_exc())

    print(f"[BATCH {batch_id}] STEP5b: メール送信キュー 登録開始")
    try:
        enqueue_email_notification(alert_df, batch_id, batch_df.sparkSession)
        print(f"[BATCH {batch_id}] STEP5b: メール送信キュー 登録完了")
    except Exception as e:
        print(f"[SILVER_ERR_003] メール送信キューへの書込みに失敗しました: {e}")
        import traceback
        print(traceback.format_exc())

    print(f"[BATCH {batch_id}] STEP5b: デバイスステータス 更新開始")
    try:
        update_device_status(alert_df, batch_id)
        print(f"[BATCH {batch_id}] STEP5b: デバイスステータス 更新完了")
    except Exception as e:
        print(f"[SILVER_ERR_005] デバイスステータスの更新に失敗しました: {e}")
        import traceback
        print(traceback.format_exc())

    print(f"[BATCH {batch_id}] ===== foreachBatch 終了 =====")


# =============================================================================
# パイプライン起動
# =============================================================================

def build_kafka_stream():
    """
    Kafkaストリームを構築し、デバイスID抽出・フォーマット変換・JSONパースを適用する

    Returns:
        DataFrame: センサーデータ（パース済み）
    """
    return (
        spark.readStream
        .format("kafka")
        .options(**kafka_options)
        .load()

        # STEP 1: Kafkaカラム選択
        .select(
            F.col("value"),
            F.col("topic"),
            F.col("key").cast("string").alias("message_key"),
            F.col("timestamp").alias("kafka_timestamp"),
        )

        # STEP 1.5: デバイスID抽出
        .withColumn(
            "extracted_device_id",
            extract_device_id_udf(
                F.col("topic"),
                F.col("message_key"),
                F.get_json_object(F.col("value").cast("string"), "$.device_id"),
            )
        )
        .filter(F.col("extracted_device_id").isNotNull())

        # STEP 1.6: バイナリ/JSON変換
        .withColumn(
            "raw_json",
            convert_to_json_with_device_id_udf(
                F.col("value"),
                F.col("extracted_device_id"),
            )
        )
        .filter(F.col("raw_json").isNotNull() & F.col("raw_json").rlike(r'^\{.*\}'))

        # STEP 2: JSONパース
        .withColumn("parsed", F.from_json(F.col("raw_json"), sensor_schema))
        .filter(F.col("parsed").isNotNull())
        .filter(F.col("parsed.device_id").isNotNull())
        .filter(F.col("parsed.event_timestamp").isNotNull())
        .select(
            F.col("parsed.device_id").alias("device_id"),
            F.to_timestamp(F.col("parsed.event_timestamp")).alias("event_timestamp"),
            F.col("parsed.external_temp").alias("external_temp"),
            F.col("parsed.set_temp_freezer_1").alias("set_temp_freezer_1"),
            F.col("parsed.internal_sensor_temp_freezer_1").alias("internal_sensor_temp_freezer_1"),
            F.col("parsed.internal_temp_freezer_1").alias("internal_temp_freezer_1"),
            F.col("parsed.df_temp_freezer_1").alias("df_temp_freezer_1"),
            F.col("parsed.condensing_temp_freezer_1").alias("condensing_temp_freezer_1"),
            F.col("parsed.adjusted_internal_temp_freezer_1").alias("adjusted_internal_temp_freezer_1"),
            F.col("parsed.set_temp_freezer_2").alias("set_temp_freezer_2"),
            F.col("parsed.internal_sensor_temp_freezer_2").alias("internal_sensor_temp_freezer_2"),
            F.col("parsed.internal_temp_freezer_2").alias("internal_temp_freezer_2"),
            F.col("parsed.df_temp_freezer_2").alias("df_temp_freezer_2"),
            F.col("parsed.condensing_temp_freezer_2").alias("condensing_temp_freezer_2"),
            F.col("parsed.adjusted_internal_temp_freezer_2").alias("adjusted_internal_temp_freezer_2"),
            F.col("parsed.compressor_freezer_1").alias("compressor_freezer_1"),
            F.col("parsed.compressor_freezer_2").alias("compressor_freezer_2"),
            F.col("parsed.fan_motor_1").alias("fan_motor_1"),
            F.col("parsed.fan_motor_2").alias("fan_motor_2"),
            F.col("parsed.fan_motor_3").alias("fan_motor_3"),
            F.col("parsed.fan_motor_4").alias("fan_motor_4"),
            F.col("parsed.fan_motor_5").alias("fan_motor_5"),
            F.col("parsed.defrost_heater_output_1").alias("defrost_heater_output_1"),
            F.col("parsed.defrost_heater_output_2").alias("defrost_heater_output_2"),
            F.col("raw_json"),
        )

        # STEP 3: デバイスマスタ結合（organization_id付与）
        .join(F.broadcast(get_device_master()), "device_id", "left")

        # STEP 3.5: 組織マスタ存在チェック（organization_idが存在しないレコードは除外）
        .join(
            F.broadcast(get_organization_master().select("organization_id")),
            "organization_id",
            "inner",
        )
    )


def diagnose_kafka_stream():
    """
    Kafkaストリームの各フィルタステップで件数を確認する診断関数。
    foreachBatch が呼ばれない原因の特定に使用する。
    """
    print("[DIAGNOSE] ===== Kafka診断開始 =====")

    # バッチ読み込みは "latest" 不可のため "earliest" に上書き
    batch_kafka_options = {**kafka_options, "startingOffsets": "earliest"}
    raw_df = spark.read.format("kafka").options(**batch_kafka_options).load()
    print(f"[DIAGNOSE] STEP1 Raw Kafka: {raw_df.count()}件")

    s1 = raw_df.select(
        F.col("value"),
        F.col("topic"),
        F.col("key").cast("string").alias("message_key"),
        F.col("timestamp").alias("kafka_timestamp"),
    )
    print(f"[DIAGNOSE] STEP1 select後: {s1.count()}件")

    s2 = s1.withColumn(
        "extracted_device_id",
        extract_device_id_udf(
            F.col("topic"),
            F.col("message_key"),
            F.get_json_object(F.col("value").cast("string"), "$.device_id"),
        )
    )
    print(f"[DIAGNOSE] STEP1.5 device_id抽出後（フィルタ前）: {s2.count()}件")
    s2f = s2.filter(F.col("extracted_device_id").isNotNull())
    print(f"[DIAGNOSE] STEP1.5 device_idフィルタ後: {s2f.count()}件")

    s3 = s2f.withColumn(
        "raw_json",
        convert_to_json_with_device_id_udf(F.col("value"), F.col("extracted_device_id"))
    )
    print(f"[DIAGNOSE] STEP1.6 JSON変換後（フィルタ前）: {s3.count()}件")
    s3f = s3.filter(F.col("raw_json").isNotNull() & F.col("raw_json").rlike(r'^\{.*\}'))
    print(f"[DIAGNOSE] STEP1.6 JSONフィルタ後: {s3f.count()}件")

    s4 = s3f.withColumn("parsed", F.from_json(F.col("raw_json"), sensor_schema))
    print(f"[DIAGNOSE] STEP2 JSONパース後（フィルタ前）: {s4.count()}件")
    s4f = (
        s4.filter(F.col("parsed").isNotNull())
          .filter(F.col("parsed.device_id").isNotNull())
          .filter(F.col("parsed.event_timestamp").isNotNull())
    )
    print(f"[DIAGNOSE] STEP2 パースフィルタ後: {s4f.count()}件")

    print("\n[DIAGNOSE] --- raw_json サンプル（3件）---")
    if s3f.count() > 0:
        s3f.select("topic", "message_key", "raw_json").show(3, truncate=100)
    else:
        print("[DIAGNOSE] raw_json が空のためサンプル表示不可")
        print("\n[DIAGNOSE] --- Kafka raw value サンプル（3件）---")
        s1.select("topic", "message_key", F.col("value").cast("string").alias("value_str")).show(3, truncate=200)

    print("[DIAGNOSE] ===== Kafka診断終了 =====")


def start_pipeline():
    """パイプライン起動"""
    print("[PIPELINE] ストリーム構築開始")
    kafka_stream = build_kafka_stream()
    print("[PIPELINE] ストリーム構築完了")

    print("[PIPELINE] writeStream 開始")
    query = (
        kafka_stream
        .writeStream
        .foreachBatch(process_sensor_batch)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime="10 seconds")
        .start()
    )
    print(f"[PIPELINE] クエリ起動完了: id={query.id}, runId={query.runId}")

    query.awaitTermination()
    print("[PIPELINE] クエリ終了")


# =============================================================================
# バッチモード検証（readStream が動作しない場合の代替テスト）
# =============================================================================

def run_batch_test():
    """
    Kafka をバッチ読み込みして process_sensor_batch を直接呼び出す。
    readStream が動作しない環境での動作確認に使用する。
    """
    print("[BATCH_TEST] ===== バッチテスト開始 =====")

    raw_df = spark.read.format("kafka").options(**kafka_options).load()
    print(f"[BATCH_TEST] Kafka 読込件数: {raw_df.count()}件")

    batch_df = (
        raw_df
        .select(
            F.col("value"),
            F.col("topic"),
            F.col("key").cast("string").alias("message_key"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn(
            "extracted_device_id",
            extract_device_id_udf(
                F.col("topic"),
                F.col("message_key"),
                F.get_json_object(F.col("value").cast("string"), "$.device_id"),
            )
        )
        .filter(F.col("extracted_device_id").isNotNull())
        .withColumn(
            "raw_json",
            convert_to_json_with_device_id_udf(F.col("value"), F.col("extracted_device_id"))
        )
        .filter(F.col("raw_json").isNotNull() & F.col("raw_json").rlike(r'^\{.*\}'))
        .withColumn("parsed", F.from_json(F.col("raw_json"), sensor_schema))
        .filter(F.col("parsed").isNotNull())
        .filter(F.col("parsed.device_id").isNotNull())
        .filter(F.col("parsed.event_timestamp").isNotNull())
        .select(
            F.col("parsed.device_id").alias("device_id"),
            F.to_timestamp(F.col("parsed.event_timestamp")).alias("event_timestamp"),
            F.col("parsed.external_temp").alias("external_temp"),
            F.col("parsed.set_temp_freezer_1").alias("set_temp_freezer_1"),
            F.col("parsed.internal_sensor_temp_freezer_1").alias("internal_sensor_temp_freezer_1"),
            F.col("parsed.internal_temp_freezer_1").alias("internal_temp_freezer_1"),
            F.col("parsed.df_temp_freezer_1").alias("df_temp_freezer_1"),
            F.col("parsed.condensing_temp_freezer_1").alias("condensing_temp_freezer_1"),
            F.col("parsed.adjusted_internal_temp_freezer_1").alias("adjusted_internal_temp_freezer_1"),
            F.col("parsed.set_temp_freezer_2").alias("set_temp_freezer_2"),
            F.col("parsed.internal_sensor_temp_freezer_2").alias("internal_sensor_temp_freezer_2"),
            F.col("parsed.internal_temp_freezer_2").alias("internal_temp_freezer_2"),
            F.col("parsed.df_temp_freezer_2").alias("df_temp_freezer_2"),
            F.col("parsed.condensing_temp_freezer_2").alias("condensing_temp_freezer_2"),
            F.col("parsed.adjusted_internal_temp_freezer_2").alias("adjusted_internal_temp_freezer_2"),
            F.col("parsed.compressor_freezer_1").alias("compressor_freezer_1"),
            F.col("parsed.compressor_freezer_2").alias("compressor_freezer_2"),
            F.col("parsed.fan_motor_1").alias("fan_motor_1"),
            F.col("parsed.fan_motor_2").alias("fan_motor_2"),
            F.col("parsed.fan_motor_3").alias("fan_motor_3"),
            F.col("parsed.fan_motor_4").alias("fan_motor_4"),
            F.col("parsed.fan_motor_5").alias("fan_motor_5"),
            F.col("parsed.defrost_heater_output_1").alias("defrost_heater_output_1"),
            F.col("parsed.defrost_heater_output_2").alias("defrost_heater_output_2"),
            F.col("raw_json"),
        )
        .join(F.broadcast(get_device_master()), "device_id", "left")
        .join(
            F.broadcast(get_organization_master().select("organization_id")),
            "organization_id",
            "inner",
        )
    )

    print(f"[BATCH_TEST] 変換後件数: {batch_df.count()}件")
    process_sensor_batch(batch_df, 0)

    # MySQL件数確認
    print("[BATCH_TEST] MySQL silver_sensor_data 件数確認")
    try:
        count_df = (
            builtins.spark.read.format("jdbc")
            .options(
                url=OLTP_JDBC_URL,
                dbtable="(SELECT COUNT(*) AS cnt FROM silver_sensor_data) t",
                user=_dbutils_proxy.secrets.get("my_sql_secrets", "username"),
                password=_dbutils_proxy.secrets.get("my_sql_secrets", "password"),
            )
            .load()
        )
        print(f"[BATCH_TEST] silver_sensor_data 件数: {count_df.collect()[0]['cnt']}")
    except Exception as e:
        print(f"[BATCH_TEST] MySQL件数確認失敗: {e}")

    print("[BATCH_TEST] ===== バッチテスト終了 =====")


# =============================================================================
# エントリーポイント（Databricks ノートブック実行時）
# =============================================================================

if __name__ == "__main__":
    start_pipeline()  # 常駐ストリーミングとして起動（Databricks Job 用）