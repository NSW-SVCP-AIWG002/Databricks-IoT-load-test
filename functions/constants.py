import builtins as _builtins
import random
import certifi as _certifi

# =============================================================================
# パイプライン設定
# =============================================================================

PIPELINE_TRIGGER_INTERVAL = "10 seconds"
TOPIC_NAME = "eh-telemetry"

# =============================================================================
# バイナリフォーマット定義
# =============================================================================

BINARY_STRUCT_FORMAT = "<iq22d"
BINARY_DATA_SIZE = 188  # バイト数: INT32(4) + INT64(8) + FLOAT64*22(176) = 188

# センサーフィールド名（バイナリアンパック順序と対応）
SENSOR_FIELDS = [
    "external_temp",
    "set_temp_freezer_1",
    "internal_sensor_temp_freezer_1",
    "internal_temp_freezer_1",
    "df_temp_freezer_1",
    "condensing_temp_freezer_1",
    "adjusted_internal_temp_freezer_1",
    "set_temp_freezer_2",
    "internal_sensor_temp_freezer_2",
    "internal_temp_freezer_2",
    "df_temp_freezer_2",
    "condensing_temp_freezer_2",
    "adjusted_internal_temp_freezer_2",
    "compressor_freezer_1",
    "compressor_freezer_2",
    "fan_motor_1",
    "fan_motor_2",
    "fan_motor_3",
    "fan_motor_4",
    "fan_motor_5",
    "defrost_heater_output_1",
    "defrost_heater_output_2",
]

# =============================================================================
# MySQL接続設定（Databricks Secrets経由で注入される想定）
# =============================================================================

def get_mysql_config():
    """
    MySQL接続設定を返す（遅延解決）

    Spark Connect ワーカーではモジュールインポート時に dbutils が利用不可なため、
    実行時に builtins.dbutils 経由で解決する。
    """
    _dbutils = _builtins.dbutils
    return {
        "host": _dbutils.secrets.get("my_sql_secrets", "host"),
        "port": 3306,
        "user": _dbutils.secrets.get("my_sql_secrets", "username"),
        "password": _dbutils.secrets.get("my_sql_secrets", "password"),
        "database": "iot_app_db",
        "charset": "utf8mb4",
        "connect_timeout": 10,
        "read_timeout": 30,
        "write_timeout": 30,
        "ssl_ca": _certifi.where(),    # certifi CA証明書でSSL有効化（Azure MySQL対応）
        "ssl_verify_cert": True,
        "ssl_verify_identity": False,  # ホスト名検証はスキップ
    }

# ドライバーではインポート時に初期化、ワーカーではスキップ
try:
    MYSQL_CONFIG = get_mysql_config()
except (NameError, AttributeError):
    MYSQL_CONFIG = None

# =============================================================================
# OLTPリトライ設定
# =============================================================================

OLTP_MAX_RETRIES = 3

# ジッター付き指数バックオフ（秒）: 1-2秒, 2-4秒, 4-8秒
OLTP_RETRY_INTERVALS = [
    random.uniform(1, 2),
    random.uniform(2, 4),
    random.uniform(4, 8),
]

# リトライ対象MySQLエラーコード
RETRYABLE_MYSQL_ERRNOS = {
    2003,  # Can't connect to MySQL server
    2006,  # MySQL server has gone away
    2013,  # Lost connection to MySQL server during query
}
