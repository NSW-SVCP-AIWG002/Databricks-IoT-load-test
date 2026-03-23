import socket
import time
from contextlib import contextmanager

import pymysql

from functions.constants import (
    get_mysql_config,
    OLTP_MAX_RETRIES,
    OLTP_RETRY_INTERVALS,
    RETRYABLE_MYSQL_ERRNOS,
)

# リトライ対象の例外
RETRYABLE_EXCEPTIONS = (
    socket.timeout,
    ConnectionResetError,
    BrokenPipeError,
    OSError,
)


def is_retryable_error(error: Exception) -> bool:
    """
    リトライ可能なエラーかどうかを判定する

    リトライ対象:
    - 一時的なネットワークエラー（socket.timeout, ConnectionResetError, BrokenPipeError, OSError）
    - MySQL接続系エラー（errno: 2003, 2006, 2013）

    Args:
        error: 判定対象の例外

    Returns:
        bool: リトライ可能ならTrue
    """
    if isinstance(error, RETRYABLE_EXCEPTIONS):
        return True

    if isinstance(error, pymysql.err.OperationalError):
        errno = error.args[0] if error.args else None
        if errno in RETRYABLE_MYSQL_ERRNOS:
            return True

    return False


@contextmanager
def get_mysql_connection():
    """
    リトライ付きMySQL接続コンテキストマネージャ

    接続失敗時にジッター付き指数バックオフでリトライを行う。
    最大リトライ回数を超えた場合は例外を再送出する。

    使用例:
        with get_mysql_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT ...")
            conn.commit()
    """
    conn = None
    last_error = None

    for attempt in range(OLTP_MAX_RETRIES + 1):
        try:
            conn = pymysql.connect(**get_mysql_config())
            yield conn
            return

        except pymysql.Error as e:
            last_error = e

            if attempt < OLTP_MAX_RETRIES - 1:
                wait_time = OLTP_RETRY_INTERVALS[attempt]
                print(f"MySQL接続エラー（試行 {attempt + 1}/{OLTP_MAX_RETRIES}）: {e}")
                print(f"{wait_time}秒後にリトライします...")
                time.sleep(wait_time)
            else:
                print(f"MySQL接続エラー: 最大リトライ回数（{OLTP_MAX_RETRIES}回）を超過しました")
                raise last_error

        finally:
            if conn:
                conn.close()
