# spark-app: Druid → IoTDB 데이터 마이그레이션

Apache Spark(Kubernetes Operator)를 이용해 Apache Druid에 적재된 시계열 데이터를 Apache IoTDB로 이관하는 배치 애플리케이션입니다.

## 개요

- Druid Broker(Avatica JDBC)에서 지정한 패턴의 테이블 목록을 조회
- 테이블별로 `__time`, `OriTime`, `Val` 컬럼을 읽어 IoTDB tree-model 스키마(`Time`, `Device`, measurement 컬럼)로 변환
- `org.apache.iotdb.spark.db` (IoTDB Spark connector)를 통해 IoTDB에 적재
- Kubernetes 상의 `SparkApplication` (Spark Operator) 리소스로 실행

## 아키텍처

```
Druid Broker (Avatica JDBC)
        │
        ▼
   Spark (Driver/Executor, K8s)
        │
        ▼
IoTDB (jdbc-balancer, tree-model insert)
```

## 사전 요구사항

- Kubernetes 클러스터 + [Spark Operator](https://github.com/kubeflow/spark-operator) 설치
- Docker 이미지 레지스트리 (사내 레지스트리 등)
- Apache Druid, Apache IoTDB 클러스터 (IoTDB 2.0.x 계열)
- 로컬 개발 시: Python 3.10+, Java 8/11/17, Apache Spark 3.5.9

## 프로젝트 구조

```
.
├── Dockerfile
├── app.py                  # 마이그레이션 메인 스크립트
├── jars/                   # IoTDB / thrift 등 의존성 jar (버전 통일 필수)
├── .env.example             # 로컬 실행용 환경변수 템플릿
├── k8s/
│   └── spark-application.example.yaml
└── README.md
```

## 의존성 jar 버전 (중요)

IoTDB 관련 jar는 서로 다른 버전이 섞이면 `NoSuchMethodError` 등 런타임 오류가 발생합니다. **`spark-iotdb-connector`가 지원하는 버전(2.0.3)으로 전체를 통일**해야 합니다.

| jar | 버전 |
|---|---|
| iotdb-jdbc | 2.0.3 |
| iotdb-session | 2.0.3 |
| iotdb-thrift / thrift-commons / thrift-confignode / thrift-consensus | 2.0.3 |
| isession | 2.0.3 |
| service-rpc | 2.0.3 |
| tsfile | 2.0.3 |
| spark-iotdb-connector_2.12 | 2.0.3 |
| libthrift | 0.23.0 |

베이스 이미지(`apache/spark`)에 기본 포함된 구버전 `libthrift-0.12.0.jar`는 Dockerfile에서 제거 후 위 버전으로 교체합니다.

## Docker 이미지 빌드

```bash
docker build -t <registry>/spark/druid-to-iotdb-app:<tag> .
docker push <registry>/spark/druid-to-iotdb-app:<tag>
```

## Kubernetes 배포

`k8s/spark-application.example.yaml`을 참고해 `SparkApplication` 매니페스트를 작성합니다. Druid/IoTDB 접속 정보는 하드코딩하지 말고 `env` 또는 Kubernetes `Secret`으로 주입하세요.

```bash
kubectl apply -f k8s/spark-application.yaml
kubectl logs -f <driver-pod-name>
```


