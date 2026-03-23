import builtins as _builtins

from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DoubleType, IntegerType, StructField, StructType, TimestampType,
)

from functions.mysql_connector import get_mysql_connection


_DATABASE = "iot_app_db"
_PORT = 3306


def _oltp_jdbc_url():
    """モジュールレベルでの dbutils 呼び出しを避け、実行時に解決する"""
    _host = _builtins.dbutils.secrets.get("my_sql_secrets", "host")
    return f"jdbc:mysql://{_host}:{_PORT}/{_DATABASE}?useSSL=true&requireSSL=true&verifyServerCertificate=true"


# フォールバック用スキーマ（実DBの全カラム + 派生カラム）
_ALERT_ABNORMAL_STATE_SCHEMA = StructType([
    StructField("device_id",           IntegerType(),   True),
    StructField("alert_id",            IntegerType(),   True),
    StructField("abnormal_start_time", TimestampType(), True),
    StructField("last_event_time",     TimestampType(), True),
    StructField("last_sensor_value",   DoubleType(),    True),
    StructField("alert_fired_time",    TimestampType(), True),
    StructField("alert_history_id",    IntegerType(),   True),
    # 派生カラム（DBには存在しない。ロード後に is_abnormal / alert_fired として付与）
    StructField("is_abnormal",         BooleanType(),   True),
    StructField("alert_fired",         BooleanType(),   True),
])


def get_alert_abnormal_state():
    """
    異常状態テーブルを OLTP DB から取得する。

    実DBテーブルのカラム:
        device_id, alert_id, abnormal_start_time, last_event_time,
        last_sensor_value, alert_fired_time, alert_history_id,
        create_time, update_time

    DBには is_abnormal / alert_fired カラムが存在しないため、
    ロード後に派生カラムとして追加する:
        is_abnormal = abnormal_start_time IS NOT NULL
        alert_fired  = alert_fired_time  IS NOT NULL
    """
    try:
        df = _builtins.spark.read.format("jdbc").options(
            url=_oltp_jdbc_url(),
            # driver="com.mysql.cj.jdbc.Driver",
            dbtable="alert_abnormal_state",
            user=_builtins.dbutils.secrets.get("my_sql_secrets", "username"),
            password=_builtins.dbutils.secrets.get("my_sql_secrets", "password"),
        ).load()
        # Spark Connect では .load() が lazy のため、ここでスキーマ解決を強制する
        _ = df.schema
        return (
            df
            .withColumn("is_abnormal", F.col("abnormal_start_time").isNotNull())
            .withColumn("alert_fired",  F.col("alert_fired_time").isNotNull())
        )
    except Exception as e:
        print(f"[WARN] alert_abnormal_state 取得失敗（空DataFrameで代替）: {str(e).splitlines()[0]}")
        return _builtins.spark.createDataFrame([], schema=_ALERT_ABNORMAL_STATE_SCHEMA)


# =============================================================================
# 測定項目IDとセンサーカラムのマッピング
# =============================================================================

MEASUREMENT_COLUMN_MAP = {
    1:  "external_temp",
    2:  "set_temp_freezer_1",
    3:  "internal_sensor_temp_freezer_1",
    4:  "internal_temp_freezer_1",
    5:  "df_temp_freezer_1",
    6:  "condensing_temp_freezer_1",
    7:  "adjusted_internal_temp_freezer_1",
    8:  "set_temp_freezer_2",
    9:  "internal_sensor_temp_freezer_2",
    10: "internal_temp_freezer_2",
    11: "df_temp_freezer_2",
    12: "condensing_temp_freezer_2",
    13: "adjusted_internal_temp_freezer_2",
    14: "compressor_freezer_1",
    15: "compressor_freezer_2",
    16: "fan_motor_1",
    17: "fan_motor_2",
    18: "fan_motor_3",
    19: "fan_motor_4",
    20: "fan_motor_5",
    21: "defrost_heater_output_1",
    22: "defrost_heater_output_2",
}


# =============================================================================
# 純粋関数（テスト可能な判定ロジック）
# =============================================================================

def determine_update_pattern(threshold_exceeded: bool, alert_triggered: bool) -> str:
    """
    閾値超過フラグとアラート発報フラグから異常状態テーブルの更新パターンを判定する

    Returns:
        "recovery"        - 復旧（閾値正常→状態リセット）
        "alert_fired"     - アラート発報（alert_fired_time=NOW()）
        "abnormal_start"  - 新規異常開始 or 異常継続
    """
    if not threshold_exceeded:
        return "recovery"
    elif alert_triggered:
        return "alert_fired"
    else:
        return "abnormal_start"


def should_enqueue_email(alert_triggered: bool, alert_email_flag: bool) -> bool:
    """
    メール送信キュー登録の対象かどうかを判定する

    Args:
        alert_triggered: アラートが発報されたか
        alert_email_flag: メール送信フラグ

    Returns:
        bool: キュー登録対象かどうか
    """
    return alert_triggered is True and alert_email_flag is True


# =============================================================================
# Spark依存関数（閾値判定・継続時間判定）
# =============================================================================

def evaluate_threshold(sensor_df, alert_settings_df):
    """
    センサーデータに対して閾値判定を実行

    Args:
        sensor_df: センサーデータDataFrame
        alert_settings_df: アラート設定マスタDataFrame

    Returns:
        DataFrame: threshold_exceeded, current_sensor_value カラムを追加したDataFrame
    """
    joined = sensor_df.join(
        F.broadcast(alert_settings_df),
        sensor_df.device_id == alert_settings_df.device_id,
        "left"
    ).drop(alert_settings_df["device_id"])

    threshold_exceeded = F.lit(False)
    sensor_value_expr = F.lit(None).cast("double")

    for item_id, column_name in MEASUREMENT_COLUMN_MAP.items():
        condition = (
            (F.col("alert_conditions_measurement_item_id") == item_id) &
            (
                F.when(F.col("alert_conditions_operator") == ">",
                       F.col(column_name) > F.col("alert_conditions_threshold"))
                .when(F.col("alert_conditions_operator") == ">=",
                      F.col(column_name) >= F.col("alert_conditions_threshold"))
                .when(F.col("alert_conditions_operator") == "<",
                      F.col(column_name) < F.col("alert_conditions_threshold"))
                .when(F.col("alert_conditions_operator") == "<=",
                      F.col(column_name) <= F.col("alert_conditions_threshold"))
                .when(F.col("alert_conditions_operator") == "==",
                      F.col(column_name) == F.col("alert_conditions_threshold"))
                .when(F.col("alert_conditions_operator") == "!=",
                      F.col(column_name) != F.col("alert_conditions_threshold"))
                .otherwise(F.lit(False))
            )
        )
        threshold_exceeded = threshold_exceeded | condition

        sensor_value_expr = F.when(
            F.col("alert_conditions_measurement_item_id") == item_id,
            F.col(column_name)
        ).otherwise(sensor_value_expr)

    return joined.withColumn("threshold_exceeded", threshold_exceeded) \
                 .withColumn("current_sensor_value", sensor_value_expr)


def check_alerts_with_duration(threshold_df, alert_state_df):
    """
    継続時間を考慮したアラート発報判定

    判定条件: 閾値超過 AND 異常継続中 AND 継続時間 >= judgment_time AND 未発報

    Args:
        threshold_df: 閾値判定済みDataFrame
        alert_state_df: 異常状態テーブルDataFrame（is_abnormal / alert_fired 派生カラム含む）

    Returns:
        DataFrame: alert_triggered カラムを追加したDataFrame
    """
    with_state = threshold_df.join(
        alert_state_df.select(
            F.col("device_id").alias("state_device_id"),
            F.col("alert_id").alias("state_alert_id"),
            "is_abnormal",
            "abnormal_start_time",
            "alert_fired"
        ),
        (threshold_df.device_id == F.col("state_device_id")) &
        (threshold_df.alert_id == F.col("state_alert_id")),
        "left"
    )

    alert_triggered = (
        (F.col("threshold_exceeded") == True) &
        (F.col("is_abnormal") == True) &
        (
            (F.unix_timestamp(F.col("event_timestamp")) -
             F.unix_timestamp(F.col("abnormal_start_time")))
            >= (F.col("judgment_time") * 60)
        ) &
        (F.coalesce(F.col("alert_fired"), F.lit(False)) == False)
    )

    return with_state.withColumn("alert_triggered", alert_triggered)


# =============================================================================
# MySQL書き込み関数（foreachBatch呼び出し用）
# =============================================================================

def update_alert_abnormal_state(batch_df, batch_id):
    """
    異常状態テーブルの更新（foreachBatchで呼び出し）

    更新パターン:
    1. 復旧: threshold_exceeded=False → 状態リセット
    2. アラート発報: alert_triggered=True → alert_fired_time=NOW()
    3. 新規異常/継続: それ以外 → abnormal_start_time=COALESCE(既存値, event_timestamp)
    """
    latest_per_alert = batch_df.groupBy("device_id", "alert_id").agg(
        F.max("event_timestamp").alias("max_event_timestamp")
    ).alias("latest")

    update_records = batch_df.alias("batch").join(
        latest_per_alert,
        (F.col("batch.device_id") == F.col("latest.device_id")) &
        (F.col("batch.alert_id") == F.col("latest.alert_id")) &
        (F.col("batch.event_timestamp") == F.col("latest.max_event_timestamp")),
        "inner"
    ).select(
        F.col("batch.device_id"),
        F.col("batch.alert_id"),
        F.col("batch.threshold_exceeded"),
        F.col("batch.current_sensor_value"),
        F.col("batch.event_timestamp"),
        F.col("batch.alert_triggered"),
        F.col("batch.abnormal_start_time"),
    ).distinct().collect()

    if not update_records:
        return

    with get_mysql_connection() as conn:
        with conn.cursor() as cursor:
            for record in update_records:
                pattern = determine_update_pattern(
                    record["threshold_exceeded"],
                    record["alert_triggered"]
                )

                if pattern == "recovery":
                    cursor.execute("""
                        INSERT INTO alert_abnormal_state
                            (device_id, alert_id, abnormal_start_time, last_event_time,
                             last_sensor_value, alert_fired_time, alert_history_id,
                             create_time, update_time)
                        VALUES (%s, %s, NULL, %s, %s, NULL, NULL, NOW(), NOW())
                        ON DUPLICATE KEY UPDATE
                            abnormal_start_time = NULL,
                            last_event_time = VALUES(last_event_time),
                            last_sensor_value = VALUES(last_sensor_value),
                            alert_fired_time = NULL,
                            alert_history_id = NULL,
                            update_time = NOW()
                    """, (
                        record["device_id"],
                        record["alert_id"],
                        record["event_timestamp"],
                        record["current_sensor_value"]
                    ))
                elif pattern == "alert_fired":
                    cursor.execute("""
                        INSERT INTO alert_abnormal_state
                            (device_id, alert_id, abnormal_start_time, last_event_time,
                             last_sensor_value, alert_fired_time, alert_history_id,
                             create_time, update_time)
                        VALUES (%s, %s, %s, %s, %s, NOW(), NULL, NOW(), NOW())
                        ON DUPLICATE KEY UPDATE
                            last_event_time = VALUES(last_event_time),
                            last_sensor_value = VALUES(last_sensor_value),
                            alert_fired_time = NOW(),
                            update_time = NOW()
                    """, (
                        record["device_id"],
                        record["alert_id"],
                        record["abnormal_start_time"],
                        record["event_timestamp"],
                        record["current_sensor_value"]
                    ))
                else:  # abnormal_start
                    cursor.execute("""
                        INSERT INTO alert_abnormal_state
                            (device_id, alert_id, abnormal_start_time, last_event_time,
                             last_sensor_value, alert_fired_time, alert_history_id,
                             create_time, update_time)
                        VALUES (%s, %s, %s, %s, %s, NULL, NULL, NOW(), NOW())
                        ON DUPLICATE KEY UPDATE
                            abnormal_start_time = COALESCE(abnormal_start_time, VALUES(abnormal_start_time)),
                            last_event_time = VALUES(last_event_time),
                            last_sensor_value = VALUES(last_sensor_value),
                            update_time = NOW()
                    """, (
                        record["device_id"],
                        record["alert_id"],
                        record["event_timestamp"],
                        record["event_timestamp"],
                        record["current_sensor_value"]
                    ))

        conn.commit()


def enqueue_email_notification(batch_df, batch_id, spark):
    """
    メール送信キューへの登録（foreachBatchで呼び出し）

    alert_triggered=True かつ alert_email_flag=True のレコードのみ登録する
    """
    alert_records = batch_df.filter(
        (F.col("alert_triggered") == True) &
        (F.col("alert_email_flag") == True)
    )

    if alert_records.isEmpty():
        return

    mail_settings = spark.read.format("jdbc").options(
        url=_oltp_jdbc_url(),
        # driver="com.mysql.cj.jdbc.Driver",
        dbtable="mail_setting",
        user=_builtins.dbutils.secrets.get("my_sql_secrets", "username"),
        password=_builtins.dbutils.secrets.get("my_sql_secrets", "password"),
    ).load().filter("is_active = TRUE")

    measurement_items = spark.read.format("jdbc").options(
        url=_oltp_jdbc_url(),
        # driver="com.mysql.cj.jdbc.Driver",
        dbtable="measurement_item_master",
        user=_builtins.dbutils.secrets.get("my_sql_secrets", "username"),
        password=_builtins.dbutils.secrets.get("my_sql_secrets", "password"),
    ).load()

    current_time = F.current_timestamp()

    queue_records = (
        alert_records
        .join(F.broadcast(mail_settings), "organization_id", "inner")
        .join(
            F.broadcast(measurement_items),
            alert_records.alert_conditions_measurement_item_id == measurement_items.measurement_item_id,
            "left"
        )
        .select(
            F.col("device_id"),
            F.col("organization_id"),
            F.col("alert_id"),
            F.col("email_address").alias("recipient_email"),
            F.concat(
                F.lit("[アラート] "),
                F.col("alert_name"),
                F.lit(" - デバイスID: "),
                F.col("device_id").cast("string")
            ).alias("subject"),
            F.concat(
                F.lit("アラートが発生しました。\n\n"),
                F.lit("デバイスID: "), F.col("device_id").cast("string"), F.lit("\n"),
                F.lit("アラート名: "), F.col("alert_name"), F.lit("\n"),
                F.lit("測定項目: "), F.col("measurement_item_name"), F.lit("\n"),
                F.lit("検出日時: "), F.col("event_timestamp").cast("string"), F.lit("\n")
            ).alias("body"),
            F.to_json(F.struct(
                F.col("alert_id"),
                F.col("alert_name"),
                F.col("alert_conditions_measurement_item_id").alias("measurement_item_id"),
                F.col("measurement_item_name"),
                F.col("alert_conditions_operator").alias("operator"),
                F.col("alert_conditions_threshold").alias("threshold"),
                F.col("alert_level_id"),
                F.col("event_timestamp").alias("detected_at"),
                F.col("device_id"),
                F.col("organization_id")
            )).alias("alert_detail_json"),
            F.lit("PENDING").alias("status"),
            F.lit(0).alias("retry_count"),
            F.lit(None).cast("string").alias("error_message"),
            F.col("event_timestamp"),
            current_time.alias("queued_time"),
            F.lit(None).cast("timestamp").alias("processed_time"),
        )
    )

    queue_list = queue_records.collect()

    if not queue_list:
        return

    with get_mysql_connection() as conn:
        with conn.cursor() as cursor:
            for record in queue_list:
                cursor.execute("""
                    INSERT INTO email_notification_queue
                        (device_id, organization_id, alert_id, recipient_email, subject, body,
                         alert_detail_json, status, retry_count, error_message,
                         event_timestamp, queued_time, processed_time, create_time, update_time)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                """, (
                    record["device_id"],
                    record["organization_id"],
                    record["alert_id"],
                    record["recipient_email"],
                    record["subject"],
                    record["body"],
                    record["alert_detail_json"],
                    record["status"],
                    record["retry_count"],
                    record["error_message"],
                    record["event_timestamp"],
                    record["queued_time"],
                    record["processed_time"]
                ))

        conn.commit()


# =============================================================================
# アラート履歴登録（アラート発報時）
# =============================================================================

ALERT_STATUS_FIRING = 1      # 発生中
ALERT_STATUS_RECOVERED = 2   # 復旧済み


def insert_alert_history(batch_df, batch_id):
    """
    アラート履歴登録（アラート発報時）

    alert_triggered=True のレコードを alert_history に INSERT し、
    返却された alert_history_id を異常状態テーブルに保存する。
    """
    fired_records = batch_df.filter(F.col("alert_triggered") == True).collect()

    if not fired_records:
        return

    alert_history_updates = []

    with get_mysql_connection() as conn:
        with conn.cursor() as cursor:
            for record in fired_records:
                cursor.execute("""
                    INSERT INTO alert_history
                        (alert_history_uuid, alert_id, alert_occurrence_datetime,
                         alert_recovery_datetime, alert_status_id, alert_value,
                         create_date, creator, update_date, modifier, delete_flag)
                    VALUES (UUID(), %s, %s, NULL, %s, %s, NOW(), 0, NOW(), 0, FALSE)
                """, (
                    record["alert_id"],
                    record["abnormal_start_time"],
                    ALERT_STATUS_FIRING,
                    record["current_sensor_value"],
                ))

                alert_history_id = cursor.lastrowid
                alert_history_updates.append({
                    "device_id": record["device_id"],
                    "alert_id": record["alert_id"],
                    "alert_history_id": alert_history_id,
                })

            conn.commit()

            for update in alert_history_updates:
                cursor.execute("""
                    UPDATE alert_abnormal_state
                    SET alert_history_id = %s,
                        update_time = NOW()
                    WHERE device_id = %s AND alert_id = %s
                """, (
                    update["alert_history_id"],
                    update["device_id"],
                    update["alert_id"],
                ))

            conn.commit()


def update_alert_history_on_recovery(batch_df, batch_id):
    """
    アラート履歴更新（復旧時）

    閾値正常 AND アラート発報済み AND alert_history_id 存在のレコードに
    復旧日時・復旧済みステータスを記録する。
    """
    alert_state_df = get_alert_abnormal_state()

    recovery_candidates = (
        batch_df.filter(F.col("threshold_exceeded") == False)
        .select("device_id", "alert_id", "event_timestamp")
        .distinct()
    )

    recovery_records = recovery_candidates.join(
        alert_state_df.filter(
            (F.col("alert_fired_time").isNotNull()) &
            (F.col("alert_history_id").isNotNull())
        ).select("device_id", "alert_id", "alert_history_id"),
        ["device_id", "alert_id"],
        "inner"
    ).collect()

    if not recovery_records:
        return

    with get_mysql_connection() as conn:
        with conn.cursor() as cursor:
            for record in recovery_records:
                cursor.execute("""
                    UPDATE alert_history
                    SET alert_recovery_datetime = %s,
                        alert_status_id = %s,
                        update_date = NOW()
                    WHERE alert_history_id = %s
                """, (
                    record["event_timestamp"],
                    ALERT_STATUS_RECOVERED,
                    record["alert_history_id"],
                ))

        conn.commit()

    print(f"復旧処理完了: {len(recovery_records)}件のアラート履歴を更新")


def update_device_status(batch_df, batch_id):
    """
    デバイスステータス更新（foreachBatchで呼び出し）

    alert_triggered=True のレコードに対して device_status_data を UPSERT する。
    """
    alert_records = batch_df.filter(F.col("alert_triggered") == True).collect()

    if not alert_records:
        return

    with get_mysql_connection() as conn:
        with conn.cursor() as cursor:
            for record in alert_records:
                cursor.execute("""
                    INSERT INTO device_status_data
                        (device_id, latest_status, latest_event_timestamp,
                         alert_count, last_alert_timestamp, updated_at)
                    VALUES (%s, 'ALERT', %s, 1, %s, NOW())
                    ON DUPLICATE KEY UPDATE
                        latest_status = 'ALERT',
                        latest_event_timestamp = VALUES(latest_event_timestamp),
                        alert_count = alert_count + 1,
                        last_alert_timestamp = VALUES(last_alert_timestamp),
                        updated_at = NOW()
                """, (
                    record["device_id"],
                    record["event_timestamp"],
                    record["event_timestamp"],
                ))

        conn.commit()
