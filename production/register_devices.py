"""
register_devices.py - IoT Hub デバイス一括登録スクリプト

サービスプリンシパルを使用して Azure IoT Hub にロードテスト用デバイスを一括登録し、
Locust が使用するデバイス認証情報 CSV (device_credentials.csv) を生成する。

参考資料の adapter_list.csv に相当するファイルを自動生成する。

使用方法:
  # 疎通検証用 200台（device_id 1〜200）
  python register_devices.py --count 200 --offset 1

  # 本番負荷検証用 20,000台（device_id 1〜20,000）
  python register_devices.py --count 20000 --offset 1

  # 出力先を指定する場合
  python register_devices.py --count 200 --offset 1 --output device_credentials.csv

必須環境変数 (.env から自動読込):
  IOTHUB_HOSTNAME     - IoT Hub ホスト名 (例: iot-xxx.azure-devices.net)
  AZURE_TENANT_ID     - サービスプリンシパル テナントID
  AZURE_CLIENT_ID     - サービスプリンシパル クライアントID
  AZURE_CLIENT_SECRET - サービスプリンシパル クライアントシークレット

必要な IoT Hub ロール:
  サービスプリンシパルに「IoT Hub Registry Contributor」を付与すること

出力ファイル (device_credentials.csv):
  device_name, device_id, primary_key
  loadtest-device-00001, 1, {base64_key}
  loadtest-device-00002, 2, {base64_key}
  ...
"""

import argparse
import csv
import os
import sys
import time

import requests
from azure.identity import ClientSecretCredential
from dotenv import load_dotenv

load_dotenv()

IOTHUB_API_VERSION = "2021-04-12"
IOTHUB_SCOPE = "https://iothubs.azure.net/.default"
DEVICE_NAME_PREFIX = "loadtest-device"
REQUEST_TIMEOUT = 15


def _get_token(tenant_id: str, client_id: str, client_secret: str) -> str:
    """サービスプリンシパルで Azure AD トークンを取得する。"""
    credential = ClientSecretCredential(tenant_id, client_id, client_secret)
    token = credential.get_token(IOTHUB_SCOPE)
    return token.token


def _auth_header(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _get_device(iothub_hostname: str, device_name: str, token: str) -> dict | None:
    """デバイスが IoT Hub に存在するか確認し、存在すれば情報を返す。存在しない場合は None。"""
    url = f"https://{iothub_hostname}/devices/{device_name}?api-version={IOTHUB_API_VERSION}"
    resp = requests.get(url, headers=_auth_header(token), timeout=REQUEST_TIMEOUT)
    if resp.status_code == 200:
        return resp.json()
    if resp.status_code == 404:
        return None
    resp.raise_for_status()


def _create_device(iothub_hostname: str, device_name: str, token: str) -> dict:
    """IoT Hub にデバイスを新規作成し、認証情報を返す。"""
    url = f"https://{iothub_hostname}/devices/{device_name}?api-version={IOTHUB_API_VERSION}"
    body = {
        "deviceId": device_name,
        "status": "enabled",
        "authentication": {
            "type": "sas",
            "symmetricKey": {
                "primaryKey": "",    # IoT Hub が自動生成
                "secondaryKey": "",
            },
        },
    }
    resp = requests.put(
        url, headers=_auth_header(token), json=body, timeout=REQUEST_TIMEOUT
    )
    resp.raise_for_status()
    return resp.json()


def _extract_primary_key(device_info: dict) -> str:
    return device_info["authentication"]["symmetricKey"]["primaryKey"]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="IoT Hub ロードテスト用デバイス一括登録"
    )
    parser.add_argument("--count",  type=int, default=200,
                        help="登録するデバイス台数（デフォルト: 200）")
    parser.add_argument("--offset", type=int, default=1,
                        help="device_id の開始番号（デフォルト: 1）")
    parser.add_argument("--output", type=str, default="device_credentials.csv",
                        help="出力 CSV ファイルパス（デフォルト: device_credentials.csv）")
    args = parser.parse_args()

    iothub_hostname = os.environ.get("IOTHUB_HOSTNAME", "")
    tenant_id       = os.environ.get("AZURE_TENANT_ID", "")
    client_id       = os.environ.get("AZURE_CLIENT_ID", "")
    client_secret   = os.environ.get("AZURE_CLIENT_SECRET", "")

    missing = [k for k, v in {
        "IOTHUB_HOSTNAME": iothub_hostname,
        "AZURE_TENANT_ID": tenant_id,
        "AZURE_CLIENT_ID": client_id,
        "AZURE_CLIENT_SECRET": client_secret,
    }.items() if not v]
    if missing:
        print(f"[ERROR] 必須環境変数が未設定です: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)

    print(f"[INFO] IoT Hub   : {iothub_hostname}")
    print(f"[INFO] 対象範囲  : device_id {args.offset}〜{args.offset + args.count - 1}（{args.count}台）")
    print(f"[INFO] 出力先    : {args.output}")
    print()

    token = _get_token(tenant_id, client_id, client_secret)
    print("[INFO] Azure AD トークン取得完了")

    results = []
    created = skipped = failed = 0
    start_time = time.time()

    for i in range(args.count):
        device_id_int = args.offset + i
        device_name   = f"{DEVICE_NAME_PREFIX}-{device_id_int:05d}"

        try:
            existing = _get_device(iothub_hostname, device_name, token)
            if existing:
                primary_key = _extract_primary_key(existing)
                skipped += 1
            else:
                created_info = _create_device(iothub_hostname, device_name, token)
                primary_key = _extract_primary_key(created_info)
                created += 1

            results.append({
                "device_name": device_name,
                "device_id":   device_id_int,
                "primary_key": primary_key,
            })

        except Exception as e:
            print(f"[ERROR] {device_name}: {e}", file=sys.stderr)
            failed += 1

        # 進捗表示（100台ごと）
        done = i + 1
        if done % 100 == 0 or done == args.count:
            elapsed = time.time() - start_time
            print(f"  [{done:>6}/{args.count}] "
                  f"新規: {created}, 既存: {skipped}, エラー: {failed}  "
                  f"({elapsed:.1f}s)")

    # CSV 出力
    with open(args.output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["device_name", "device_id", "primary_key"])
        writer.writeheader()
        writer.writerows(results)

    print()
    print(f"[完了] 新規作成: {created}台  既存スキップ: {skipped}台  エラー: {failed}台")
    print(f"[完了] 認証情報を保存しました: {args.output}")
    if failed > 0:
        print(f"[警告] {failed}台の登録に失敗しました。再実行すると既存分をスキップして再試行します。")
        sys.exit(1)


if __name__ == "__main__":
    main()
