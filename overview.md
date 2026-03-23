# システムアーキテクチャ概要

## 概要

Databricks IoTシステムは、Azure環境上に構築された統合IoTデータプラットフォームです。Azure IoT ServicesとDatabricks Platformを組み合わせ、IoTデバイスからのデータ収集、リアルタイム処理、分析、可視化までを一貫して実現します。

**重要**: Azure IoT Hubsを含むAzure IoTプラットフォームとDatabricks Platformは、**同一のAzure環境内**にデプロイされます。これにより、Private Linkを活用したセキュアな通信、低レイテンシーなデータ連携、統一されたガバナンスを実現しています。

## システム構成図

```mermaid
graph TB
    subgraph "IoTデバイス層"
        IOT[各種IoTデバイス<br/>MQTT通信]
    end

    subgraph "Azure環境（統一環境）"
        subgraph "Azure IoT Services"
            IOTHUB[Azure IoT Hubs]
            EVENTHUB[Event Hubs<br/>Kafka Endpoint]
            CAPTURE[Event Hubs Capture]
        end

        subgraph "Azure Storage"
            ADLS[ADLS Gen2<br/>Azure Data Lake Storage]
        end

        subgraph "Databricks Platform<br/>Databricks-managed network"
            subgraph "Control Plane"
                CP[コントロールプレーン<br/>Workspace UI/API]
            end

            subgraph "Serverless Computing"
                APPS[Azure App Service<br/>Flask Application]
                LDP[Lakeflow 宣言型パイプライン<br/>Bronze/Silver/Gold]
            end

            subgraph "Classic Computing"
                CLUSTER[Compute Cluster<br/>VMSS]
            end

            subgraph "Data & Analytics"
                UC[Unity Catalog<br/>Delta Lake]
                SQLWH[SQL Warehouse]
                DASH[Databricks Dashboard]
            end
        end

        subgraph "Database"
            MYSQL[MySQL互換データベース<br/>SingleStore互換]
            VM[踏み台VM]
        end

        subgraph "ネットワーク"
            PL_EH[Private Link<br/>Event Hub]
            PL_ADLS[Private Link<br/>ADLS dfs]
            PL_MYSQL[Private Link<br/>MySQL]
            PL_DB[Private Link<br/>Databricks UI/API]
            DNS[Azure DNS<br/>Private Zones]
        end

        subgraph "認証基盤"
            ENTRA[Entra ID<br/>Azure Active Directory]
            IPRESTRICT[アクセス元IP制限]
        end
    end

    subgraph "ユーザー"
        USER[エンドユーザー<br/>Webブラウザ]
    end

    IOT -->|MQTT| IOTHUB
    IOTHUB -->|Kafka Stream| EVENTHUB
    EVENTHUB -->|継続出力| CAPTURE
    CAPTURE -->|RAWデータ保存| ADLS
    EVENTHUB -->|Stream| LDP

    LDP -->|Bronze層| ADLS
    LDP -->|Silver層| ADLS
    LDP -->|Gold層| UC
    LDP -->|ステータス更新| MYSQL

    APPS -->|クエリ| SQLWH
    APPS -->|CRUD操作| MYSQL
    SQLWH -->|データ取得| UC

    USER -->|認証| ENTRA
    ENTRA -->|認証成功| CP
    CP -->|アクセス制御| APPS
    APPS -->|iframe埋め込み| DASH

    EVENTHUB -.->|Private Link| PL_EH
    ADLS -.->|Private Link| PL_ADLS
    MYSQL -.->|Private Link| PL_MYSQL
    CP -.->|Private Link| PL_DB
    VM -.->|保守アクセス| MYSQL

    PL_EH -.-> DNS
    PL_ADLS -.-> DNS
    PL_MYSQL -.-> DNS
    PL_DB -.-> DNS

    IPRESTRICT -.->|制限| CP
    IPRESTRICT -.->|制限| APPS
```

## 主要コンポーネント

### 1. IoTデバイス層
- **各種IoTセンサー・デバイス**: MQTTプロトコルによるデータ送信
- **通信プロトコル**: MQTT over TLS 1.2+
- **デバイス規模**: 最大100,000デバイス対応

### 2. Azure IoT Services（Azure環境内）
- **Azure IoT Hubs**:
  - IoTデバイスとの双方向通信ハブ
  - デバイス認証・管理
  - MQTT → REST変換
- **Event Hubs**:
  - Kafkaエンドポイント提供
  - Databricks LDPへのストリーム配信
  - **Event Hubs Capture**: RAWデータをADLSに継続出力（長期保存・障害復旧用）
- **Private Link**: Event Hubs用Private Linkによるセキュア通信

### 3. Databricks Platform（Azure環境内、Databricks-managed network）
- **コントロールプレーン**:
  - Workspace API
  - OAuth トークン フェデレーションによるユーザー認証
  - アクセス元IP制限
- **サーバレスコンピューティング**:
  - **Lakeflow 宣言型パイプライン (LDP)**: Python/SQLによるストリーム処理
- **クラシックコンピューティング**:
  - **Compute Cluster (VMSS)**: 顧客VNet内デプロイ、バッチ処理・対話型分析
- **データ & 分析**:
  - **Unity Catalog**: Delta Lakeベースのデータカタログ、データガバナンス
  - **SQL Warehouse**: SQLクエリ実行エンジン
  - **Databricks Dashboard**: データ可視化・BIダッシュボード
- **Private Link**: ADLS (dfs) / MySQL / Databricks UI APIへのPrivate Link接続

### 4. Azure Storage（Azure環境内）
- **ADLS Gen2 (Azure Data Lake Storage)**:
  - Unity Catalogのストレージバックエンド
  - Event Hubs CaptureによるRAWデータ保存
  - Bronze/Silver層のデータ保存
  - **Private Link**: dfsエンドポイント用Private Link

### 5. アプリケーション層
- **Flask Webアプリケーション**:
  - Python 3.11ベース
  - Jinja2テンプレート（サーバーサイドレンダリング）
  - REST API提供
  - Azure App Serviceでホスティング
- **ユーザー識別**: リバースプロキシヘッダからのユーザー情報取得
- **データアクセス**:
  - Unity Catalog (SQL Warehouse経由) - IoTデータ、分析データ
  - MySQL互換データベース - マスタデータ、デバイスステータス

### 6. データ層
- **Unity Catalog (Delta Lake)**:
  - メダリオンアーキテクチャ（Bronze/Silver/Gold）
  - センサーデータの大量保存
  - 分析用データマート
  - ストレージバックエンド: ADLS Gen2
- **MySQL互換データベース (SingleStore互換)**:
  - マスタデータ（デバイス、ユーザー、アラート設定）
  - 最新デバイスステータス
  - OLTP用途
  - **Private Link**: MySQL用Private Link
  - **踏み台VM**: プライベートネットワーク内のDB保守アクセス

### 7. ネットワーク & セキュリティ（Azure環境内）
- **Private Link**:
  - Event Hub用Private Link
  - ADLS (dfs) 用Private Link
  - MySQL用Private Link
  - Databricks UI/API用Private Link
- **Azure DNS**: Private Zones（Private Link名前解決）
- **認証基盤**:
  - **認証共通モジュール**: Azure/AWS/オンプレミス環境に対応
  - **Databricks接続**: OAuth トークン フェデレーションによるユーザー単位認証とデータスコープ制御
  - **アクセス元IP制限**: Databricksワークスペース、公開フロントへのIP制限
- **リバースプロキシ**: 認証成功後、ユーザー識別子・アクセストークンをリクエストヘッダに付与

## 技術スタック

### プラットフォーム & インフラストラクチャ
- **クラウド**: Microsoft Azure（統一環境）
- **Databricks**: Premium ライセンス
- **IoT**: Azure IoT Hubs、Event Hubs
- **ストレージ**: ADLS Gen2、Delta Lake
- **データベース**: MySQL互換（SingleStore互換）
- **ネットワーク**: Private Link、Azure DNS
- **認証**: Entra ID (Azure AD)

### データ処理
- **ストリーム処理**: Lakeflow 宣言型パイプライン (LDP) - Python/SQL
- **バッチ処理**: Databricks Compute Cluster
- **データアーキテクチャ**: メダリオンアーキテクチャ（Bronze/Silver/Gold）
- **データカタログ**: Unity Catalog
- **クエリエンジン**: SQL Warehouse

### アプリケーション開発
- **プログラミング言語**: Python 3.11+
- **Webフレームワーク**: Flask
- **テンプレートエンジン**: Jinja2
- **データアクセス**:
  - @databricks/sql (SQL Warehouse接続)
  - PyMySQL (MySQL互換DB接続)
- **実行環境**: Azure App Service (App Compute)

### 通信プロトコル
- **IoTデバイス通信**: MQTT over TLS
- **ストリーミング**: Kafka (Event Hubs)
- **API**: REST (Flask)
- **認証**: OAuth 2.0 (Entra ID)

## データフロー全体図

```mermaid
graph LR
    subgraph "データ収集"
        A[IoTデバイス<br/>MQTT] --> B[IoT Hubs]
        B --> C[Event Hubs<br/>Kafka]
    end

    subgraph "データ永続化（RAW）"
        C --> D[Event Hubs<br/>Capture]
        D --> E[ADLS<br/>RAWデータ]
    end

    subgraph "メダリオンアーキテクチャ（LDP）"
        C --> F[Bronze層<br/>生データ保存<br/>アラート判定]
        F --> G[Silver層<br/>構造化・変換<br/>異常検知]
        G --> H[Gold層<br/>BI用データ]

        F -.->|ADLS<br/>Private Link| ADLS1[ADLS Gen2]
        G -.->|ADLS<br/>Private Link| ADLS1
        H -.->|Unity Catalog| UC[Delta Lake]
    end

    subgraph "OLTP更新"
        G --> I[MySQL互換DB<br/>ステータス更新]
    end

    subgraph "Webアプリケーション"
        J[Flask App] -->|SQL Warehouse| UC
        J -->|PyMySQL<br/>Private Link| I
        J --> K[Jinja2<br/>HTML生成]
    end

    subgraph "データ可視化"
        L[Databricks<br/>Dashboard] --> UC
        K -->|iframe埋め込み| L
    end

    subgraph "ユーザーアクセス"
        M[エンドユーザー] -->|Entra ID認証| N[Azure App Service]
        N --> J
    end
```

## 関連ドキュメント

- [フロントエンドアーキテクチャ](./frontend.md)
- [バックエンドアーキテクチャ](./backend.md)
- [インフラストラクチャ設計](./infrastructure.md)
- [データモデル](./data-models/)
- [アーキテクチャ決定記録 (ADR)](./decisions/)

---

## 編集履歴

| 日付       | バージョン | 編集者 | 変更内容                                                                                                    |
| ---------- | ---------- | ------ | ----------------------------------------------------------------------------------------------------------- |
| 2025-11-26 | 1.0        | Claude | 初版作成: Azure環境統合（Azure IoT + Databricks同一環境）の明確化、システム全体構成図とデータフロー図の追加 |
| 2025-11-26 | 1.1        | Claude | 環境構成セクション削除: 環境構成詳細はinfrastructure.mdに集約（overview.mdはシステム全体像に集中）          |
