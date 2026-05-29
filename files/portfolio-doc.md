# 이재영 — 프로젝트 포트폴리오

> Backend Engineer · SSAFY 14기 (빅데이터·분산 처리 트랙)
> 연락: jaeyeong221@gmail.com · GitHub: https://github.com/ljy0221

---

## 프로젝트 요약

### 🦋 Butterfly CRM — 멀티테넌트 마케팅 자동화 플랫폼

| | |
|---|---|
| **기간·규모** | 2026.03 ~ 2026.04 · 6인 · 6주 |
| **역할** | 분석·집계 파이프라인 / 발송 추적 / E2E 성능 검증 / 관측성 인프라 |
| **스택** | Java 21, Spring Boot 3.5, MySQL 8, Kafka, Redis, Grafana/Loki/Prometheus, K3s |

**주요 기여 및 성과**

- **인덱스 5개 추가로 쿼리 3곳 동시 해결** — `aggregateHourly` 3,743ms → 2.8ms **(1,314배)**, PUSH 수신자 조회 4.4배, 만료 배치 2.4배. 코드 수정 0줄, EXPLAIN 분석으로 원인 특정
- **Hibernate JDBC 배치 비활성화 원인 추적** — `GenerationType.IDENTITY`가 배치를 자동 비활성화함을 발견, JPA 우회 bulk INSERT + Kafka 배치 리스너 전환으로 TPS 16.2/s → 150.0/s **(+826%)**
- **멱등성 키 100% NULL 계약 불일치 탐지** — 상류가 키를 생성하지 않아 하류 방어 코드 무력화, 파이프라인 역추적 후 스키마 NOT NULL 강제로 근본 해결
- **k6 부하 테스트 환경 구축 + SLA 명문화** — 시나리오 3개, K3s 실측 p95 20.7ms·실패율 0%, 운영 결함 2건 사전 발견
- **Grafana/Loki/Prometheus 3축 관측성 스택 구축** — requestId MDC 전파, JSON 구조화 로그, cardinality 금지 규칙 BLOCKER 지정
- **ADR 19건 작성·기여**

---

### 🤖 AiRadar — 실시간 AI 기술 인텔리전스 플랫폼

| | |
|---|---|
| **기간·규모** | 2026.02 ~ 2026.03 · 6인 · 6주 · **팀장** |
| **역할** | Medallion EtLT 파이프라인 단독 설계·구현 / AI 서버 / Spring Boot 백엔드 |
| **스택** | Kafka, Spark 3.5, Airflow 2.8, Delta Lake, FastAPI, pgvector, Redis, Spring Boot 3.3 |

**주요 기여 및 성과**

- **Medallion EtLT 파이프라인 단독 설계·구현** — Bronze(원본 적재) → Silver(정제·중복 제거) → Gold(집계·태깅) 3단계. Kafka `AvailableNow()` Trigger로 증분 처리, 재처리 기준점 확보. Spark Job 4개·Airflow DAG 7개 단독 구현, 일 ~1,800건·~30MB 처리
- **Spark OOM 장애 원인 추적 및 해결** — `docker stats` → Spark UI → `spark-defaults.conf` 역추적으로 executor + overhead + Python worker + OS 메모리 합산 초과 확인. Docker 메모리 한도 상향, JVM 가용 메모리 4항목 명시적 분리 설정으로 OOM Kill 재발 0건
- **4단계 Fallback 추천 아키텍처** — Redis ~1ms → ALS ~50ms → PostgreSQL 교집합 ~100ms → Cold Start 순차 폴백, 이벤트 API < 50ms
- **인프라 장애 8건 직접 해결, 재발 0건**

---

### 📝 SYNAPSE — 개발자 특화 실시간 협업 노트

| | |
|---|---|
| **기간·규모** | 2026.01 ~ 2026.02 · 6인 · 6주 |
| **역할** | 백엔드 / DB 설계 / CRDT 버그 추적·해결 / 코드 실행 세션 최적화 / Electron IPC 연동 |
| **스택** | Spring Boot 3.x, Yjs CRDT, WebSocket, MongoDB, PostgreSQL, Redis, Electron |

**주요 기여 및 성과**

- **CRDT 무한 reflow 버그 추적·해결** — 서버가 발신자 origin을 구분하지 않아 자신의 Delta를 재수신 → 무한 루프 발생. Yjs `origin` 태그로 발신자 식별, 서버발 Delta 재전송 차단으로 완전 해소
- **코드 실행 세션 설계 결함 수정** — 매 실행마다 Docker 컨테이너 생성(~3,000ms)하는 구조적 결함 발견. 노트별 세션 Redis 등록·재사용 구조로 전환, 연속 실행 속도 **약 30배 개선** (~100ms)
- **다중 DB 설계** — PostgreSQL(메타·권한·관계) + MongoDB(CRDT Delta 문서) + Redis(세션·캐시) 역할 분리 설계
- **Electron IPC 로컬 Docker 연동** — Renderer → IPC → Main Process → 로컬 Docker 실행, 미설치 환경 서버 폴백 자동 전환

---

## 종합 핵심 수치

| 프로젝트 | 대표 성과 |
|---|---|
| **Butterfly CRM** | 쿼리 **1,314배** / TPS **+826%** / p95 **20.7ms** SLA 통과 / ADR **19건** |
| **AiRadar** | 파이프라인 일 **1,800건** 단독 구현 / 장애 **8건** 해결·재발 0건 |
| **SYNAPSE** | 코드 실행 **30배** 개선 / CRDT 무한 루프 해소 / 3DB 설계 |

---

<!-- 2페이지 이후: 프로젝트 상세 -->

## 상세 내용

---

## 1. Butterfly CRM

### 프로젝트 개요

| 항목 | 내용 |
|---|---|
| **기간** | 2026.03 ~ 2026.04 (6주) |
| **팀 구성** | 6인 (도메인별 역할 분담) |
| **담당 역할** | 분석·집계 파이프라인 / 발송 추적 / E2E 성능 검증 / 관측성 인프라 |
| **기술 스택** | Java 21, Spring Boot 3.5, MySQL 8, Kafka, Redis, Grafana / Loki / Prometheus, K3s |

**서비스 개요**
- 고객 행동 이벤트 수집 → 세그먼트 계산 → 캠페인 실행 → 성과 분석을 단일 플랫폼으로 처리하는 멀티테넌트 마케팅 자동화 SaaS

---

### 핵심 기여

#### 1-1. 도메인 경계를 지키면서 실시간 집계 달성 — Kafka 토픽 계약 설계

**배경 및 문제**
- analytics 모듈이 발송 결과를 집계하려면 notification 모듈의 데이터 접근이 필요
- 팀 내 "그냥 notification DB 직접 조회하면 안 되나?" 의견 존재
- 직접 조회 시: 두 모듈 스키마 결합 → notification 스키마 변경 시 analytics 동시 장애 → 배포 순서 제약 → 사실상 "2모듈 모놀리스"

**설계 결정**
- notification 워커가 발송 완료 시 `DeliveryTrackingEvent`를 Kafka에 발행
- analytics 컨슈머가 수신해 집계 테이블 UPSERT
- 두 도메인은 Kafka 토픽 계약으로만 연결, 스키마 결합 없음

```
butterfly-worker
  └─ DeliveryResultPersister → DeliveryTrackingProducer
                                      ↓ [delivery-tracking topic]
                               butterfly-reporting
                                  └─ DeliveryTrackingConsumer → CampaignStatsHourlyRepository.upsert()
```

**결과**
- 도메인 경계 유지 + 5분 주기 실시간 집계 달성
- notification 스키마 변경 시 analytics 코드 무수정
- ADR-0014 (모듈 간 직접 의존 차단), ADR-0007 (2단 집계 구조) 작성

---

#### 1-2. 집계 쿼리 풀 테이블 스캔 → 인덱스 5개로 3곳 동시 해결 (1,314배 개선)

**문제 발견 과정**
- 5분마다 실행되는 `aggregateHourly()` 배치의 실제 운영 성능이 궁금해 직접 EXPLAIN 실행
- 103만 건(1,020,786 rows) 환경에서 측정

```sql
EXPLAIN SELECT project_id, campaign_id, channel, COUNT(*)
FROM notification_logs
WHERE created_at >= NOW() - INTERVAL 1 HOUR
GROUP BY project_id, campaign_id, channel;
-- type: ALL | rows: 1,030,302 | 실행 시간: 3,743ms
```

- `type: ALL` 확인 → 100만 건 전체 풀 스캔
- 같은 방식으로 PUSH 발송 수신자 조회, 세그먼트 만료 배치도 점검 → 동일하게 풀 스캔

**원인 분석**
- `aggregateHourly`: `created_at` 범위 필터 인덱스 없음 → 전체 스캔
- PUSH 수신자 조회: `users.push_opt_in` 필터 인덱스 없음 → project_id로 뽑은 5만 건 중 10%만 통과, 90% 버림
- 세그먼트 만료 배치: `segment_members.expires_at` 인덱스 없음 → 전체 스캔으로 만료 행 탐색

**해결 — 인덱스 5개 추가, Flyway 마이그레이션으로 관리**

```sql
-- V22__add_performance_indexes.sql
ALTER TABLE notification_logs
  ADD INDEX idx_notification_logs_time_campaign (created_at, campaign_id, channel);
ALTER TABLE users ADD INDEX idx_users_project_push_opt_in (project_id, push_opt_in);
ALTER TABLE users ADD INDEX idx_users_project_email (project_id, email(100));
ALTER TABLE users ADD INDEX idx_users_project_phone (project_id, phone);
ALTER TABLE segment_members ADD INDEX idx_segment_members_expires_at (expires_at);
```

- 인덱스 설계 원칙: WHERE 절 range 조건을 인덱스 왼쪽에, GROUP BY 컬럼을 포함해 filesort 제거
- `push_token(200)` prefix 인덱스로 B-tree 크기 최소화하면서 커버링 인덱스 효과 확보

**결과 (103만 건 실측)**

| 쿼리 | Before | After | 개선 |
|---|---|---|---|
| `aggregateHourly` 집계 | 3,743ms | 2.8ms | **1,314배** |
| PUSH 수신자 조회 | 27.4ms | 6.2ms | **4.4배** |
| 세그먼트 만료 배치 | 1.8ms | 0.75ms | **2.4배** |

- 수정한 코드: **0줄** — EXPLAIN으로 원인 확인 후 인덱스만 추가

---

#### 1-3. Hibernate 배치가 조용히 꺼져 있던 원인 추적 → TPS 826% 향상

**문제 발견**
- N=30,000 부하 테스트(LT-05)에서 `notification-send-result` 컨슈머 TPS가 **16.2/s 상수로 고정**
- `application.yml`에 `hibernate.jdbc.batch_size: 1000`, `order_inserts: true` 명시되어 있음에도 작동 안 함

**원인 추적**

```java
@Id
@GeneratedValue(strategy = GenerationType.IDENTITY)  // 원인
private Long id;
```

- `GenerationType.IDENTITY` = MySQL `AUTO_INCREMENT`
- INSERT 직후 DB가 생성한 id를 즉시 읽어야 하므로 **Hibernate가 JDBC 배치를 자동 비활성화**
- `saveAll()`을 사용해도 내부에서 1건씩 INSERT 실행
- `batch_size` 설정 자체가 이 테이블에 무효화된 상태
- 추가 구조 문제: 레코드 1건씩 수신 → 1건씩 트랜잭션 커밋 → N=30,000이면 커밋 30,000회

**해결**
- JPA 우회 → `NamedParameterJdbcTemplate.batchUpdate()`로 직접 bulk INSERT
- Kafka 컨슈머를 배치 리스너로 전환, poll당 최대 500건을 단일 트랜잭션으로 처리
- `INSERT IGNORE` 사용: `message_key` 중복 시 해당 row만 건너뛰고 배치 전체 롤백 없음

```java
// Before: 레코드 1건씩
public void consume(NotificationSendResultPayload payload) {
    notificationLogRepository.save(log);  // 1 INSERT, 1 commit
}

// After: poll 배치 단위
public void consume(List<NotificationSendResultPayload> payloads) {
    bulkRepository.bulkInsert(valid);     // batchUpdate, 1 commit
}
```

**결과 (K3s 클러스터 실측)**

| 항목 | Before | After |
|---|---|---|
| INSERT 방식 | JPA save() — IDENTITY로 batch 불가 | JDBC batchUpdate() — JPA 우회 |
| 트랜잭션 커밋 | 레코드당 1회 | poll 배치당 1회 |
| N=500 기준 DB 왕복 | 500회 | 1회 |
| **TPS (N=30,000 실측)** | **16.2/s** | **150.0/s (+826%, 9.3×)** |

---

#### 1-4. 멱등성 키 100% NULL — 상류·하류 계약 불일치 탐지 및 해결

**문제 발견**
- LT-05 N=30,000 완주 후 `notification_logs.message_key` 전수 조사

```sql
SELECT COUNT(*) AS total, SUM(message_key IS NULL) AS null_keys
FROM notification_logs WHERE campaign_id = 3003;
-- total: 10,021 | null_keys: 10,021 (100%)
```

- 발송 10,000건에 row 10,021건 → 21건(0.21%) 중복 적재
- `message_key`에 UNIQUE 제약이 있으나 MySQL은 NULL을 중복으로 취급하지 않음 → 제약 완전 무력화

**원인 추적 — 파이프라인 역추적**

```
DeliveryCommand (record, 8필드)      ← messageKey 필드 없음
    ↓
DeliveryServiceImpl                  ← messageKey 참조 없음
    ↓
DeliveryResultPersister              ← NotificationLog.builder()에 .messageKey() 없음
```

- 하류 `butterfly-reporting`의 `RedisMessageKeyGuard`는 완벽하게 구현
- 그러나 상류 `butterfly-notification`이 키를 생성하지 않으니 가드가 항상 `null`로 호출
- 두 모듈 담당자가 달라 발생한 구조적 계약 불일치

**해결**
- `DeliveryCommand`에 `messageKey` 필드 추가
- 발송 커맨드 생성 시 `campaignExecutionId:userId:deviceId` 조합으로 키 생성
- 스키마 `NOT NULL`로 변경 → 미래에 동일 실수 발생 시 INSERT 실패로 즉시 탐지

```sql
ALTER TABLE notification_logs
MODIFY COLUMN message_key VARCHAR(128) NOT NULL;
```

**교훈**
- 멱등성 키 계약은 스키마 설계 시점에 `NOT NULL`로 강제해야 함
- 상류가 키를 생성하지 않으면 하류의 방어 코드는 실행조차 안 됨
- 대규모 부하 테스트는 단순 성능 측정이 아닌 **운영 안전성 검증**

---

#### 1-5. 재현 가능한 성능 검증 환경 구축 — k6 시나리오 3개, 운영 결함 2건 사전 발견

**문제**
- 팀에 성능 검증 환경 없음 → "잘 되는 것 같다" 수준의 검증

**구축 내용**

- `FakeFcmChannelAdapter`: `@ConditionalOnProperty`로 실제 FCM과 상호 배제, 50ms 고정 지연으로 외부 의존 격리 후 시스템 자체 병목만 측정
- `seed-bulk-dispatch-fixtures.sql`: users 10,000 + devices 10,000 + segments/campaigns 자동 적재
- `lt05-kafka-inject.sh N CAMPAIGN_ID USER_START`: N건 `DeliveryCommand` Kafka 직접 주입 + 완료 polling
- `lt01-event-ingest.js`: 이벤트 수집 API 100 req/s × 5분 SLA 검증
- `lt03-aggregation.js`: hourly 집계 + daily 롤업 latency 검증
- ADR-0017에 SLA 명문화: p99 < 200ms, 실패율 < 0.1%

**결과 (K3s 클러스터 실측)**

| 시나리오 | 측정값 | 기준 |
|---|---|---|
| LT-01: 이벤트 수집 API (100 req/s × 5분) | p95 **20.7ms**, 실패율 **0%**, 30,000건 | p99 < 200ms |
| LT-03: hourly 집계 | **10ms** | < 5,000ms |
| LT-03: daily 롤업 | **4ms** | < 30,000ms |
| LT-05: Kafka 배치 컨슈머 Before | TPS **16.2/s** | — |
| LT-05: Kafka 배치 컨슈머 After | TPS **150.0/s (+826%)** | — |

**발견한 운영 결함 2건**
- `message_key` 100% NULL (→ 1-4 항목으로 추적·해결)
- IP 기반 rate limit이 LT 자체를 막음 → 운영과 테스트 환경 분리 필요성 확인

---

#### 1-6. Grafana + Loki + Prometheus 3축 관측성 스택 구축

**목표**
- requestId 하나로 로그 → 메트릭 → 트레이스를 전부 조회 가능한 환경

**아키텍처**
```
AWS K3s 4-node (worker 노드)
  butterfly-api/worker/reporting Pod → /actuator/prometheus → Prometheus
  node-exporter DaemonSet           → :30090 NodePort      → Prometheus
  Promtail DaemonSet                → /var/log/pods        → Loki push

발급 EC2 (k14b101.p.ssafy.io)
  Prometheus :9090 / Loki :3100 / Grafana :3000 / Alertmanager
```

**구현 세부**
- JSON 구조화 로그: Logback `LogstashEncoder`로 stdout 출력 → Promtail이 Loki에 push
- MDC `requestId` 전파: `OncePerRequestFilter`에서 `X-Request-Id` 헤더 수신 또는 UUID 생성, 요청 전체에 전파
- cardinality 금지 규칙 팀 내 BLOCKER 지정: `userId`, `projectId`, `campaignId`를 Loki 라벨/Prometheus 레이블에 사용 금지 → 로그 본문 JSON 필드로만 사용
- Grafana Alloy 미도입 결정 (ADR-0018): Promtail EOL(2026-02-28) 인지했으나 14일 일정에서 River 문법 학습 + 마이그레이션 1.5~2일 비용 수용 불가 → trade-off 명문화

**활용 사례 — LT-05 병목 추적**
- Grafana 메트릭: JVM 힙 정상 확인 → JVM 문제 아님
- Loki LogQL: 발송 루프 안에서 건당 INFO 로그 30,000줄 발견 → 과다 로깅이 I/O 점유하는 운영 결함 발견

---

### 정량적 성과 요약

| 항목 | 수치 |
|---|---|
| `aggregateHourly` 쿼리 개선 | 3,743ms → **2.8ms (1,314배)** |
| PUSH 수신자 조회 개선 | 27.4ms → **6.2ms (4.4배)** |
| 세그먼트 만료 배치 개선 | 1.8ms → **0.75ms (2.4배)** |
| Kafka 배치 컨슈머 TPS | 16.2/s → **150.0/s (+826%, 9.3×)** |
| 이벤트 수집 API SLA (LT-01) | p95 **20.7ms**, 실패율 **0%**, 30,000건 |
| 집계 워커 latency (LT-03) | hourly **10ms** / daily **4ms** |
| ADR 작성·기여 | **19건** |
| 사전 발견 운영 결함 | **2건** |

---

## 2. AiRadar

### 프로젝트 개요

| 항목 | 내용 |
|---|---|
| **기간** | 2026.02 ~ 2026.03 (6주) |
| **팀 구성** | 6인 · 팀장 |
| **담당 역할** | Medallion EtLT 파이프라인 단독 설계·구현 / AI 서버 / Spring Boot 백엔드 / 팀 리드 |
| **기술 스택** | Kafka, Spark 3.5, Airflow 2.8, Delta Lake, FastAPI, pgvector, Redis, Spring Boot 3.3, PostgreSQL |

**서비스 개요**
- AI 기술 동향을 실시간으로 수집·분석·추천하는 인텔리전스 플랫폼
- Lambda Architecture + Medallion Architecture(Bronze→Silver→Gold) 3단계 파이프라인

**팀 분담 구조**
- 크롤러(1인) / **Medallion 파이프라인 — 본인 단독** / AI 서버 — 본인 / Spring Boot 백엔드 API(2인) / 프론트엔드(1인)

---

### 핵심 기여

#### 2-1. Medallion EtLT 파이프라인 단독 설계·구현

**EtLT 변형 채택 이유**

전통적인 ETL(Extract→Transform→Load)은 변환이 완료된 데이터만 적재한다. 그러나 AI 분석 파이프라인에서는 **원본 데이터를 일단 적재(Load)한 뒤 단계별로 변환(Transform)**해야 재처리와 감사 추적이 가능하다.

- **Extract**: 크롤러가 수집한 원본 데이터를 Kafka에 publish
- **t(소문자)**: Kafka → Bronze 적재 전 최소 정제 (스키마 파싱, 인코딩 정규화)
- **Load**: Delta Lake Bronze에 원본 형태 그대로 적재 — 재처리 기준점
- **Transform**: Bronze → Silver (중복 제거·정제) → Gold (집계·태깅) 단계별 변환

이 구조 덕분에 Silver 단계 로직이 바뀌어도 Bronze를 재처리 기준으로 전체 재실행 가능.

**Kafka 중간 버퍼 선택 이유**
- Spark Job을 크론으로 직접 트리거하면 중간 실패 시 재처리 범위 특정 불가, 중복 처리 위험
- Kafka `AvailableNow()` Trigger: 실행 시점 기준 **미처리 오프셋만 소비** → 이미 처리한 데이터 재처리 없음, 실패 시 오프셋부터 재시작

**파이프라인 구조**

```
크롤러(팀원) → Kafka [raw-articles]
                    ↓ AvailableNow() Trigger
              Spark Bronze Job  (t: 최소 정제 → Load: Delta Lake Bronze)
                    ↓
              Spark Silver Job  (Transform: 중복 제거·정제 → Delta Lake Silver)
                    ↓
              Spark Gold Job    (Transform: 집계·태깅 → Delta Lake Gold)
                    ↓
              AI 서버           (임베딩 생성 → pgvector IVFFlat)
                    ↓
              Spring Boot API   (추천 서빙 — 팀원)

Airflow DAG 7개: 각 단계 오케스트레이션 + 실패 알림 + 재처리 트리거
```

**구현 규모**
- Spark Job 4개 (Bronze / Silver / Gold / 임베딩) — 본인 단독 구현
- Airflow DAG 7개 — 본인 단독 구현
- 일 ~1,800건, ~30MB 처리 / 전체 사이클 ~23~50분

---

#### 2-2. Spark OOM 장애 추적 및 해결 (대표 사례)

**장애 상황**
- 배포 후 Silver Job이 간헐적으로 OOM Kill 발생
- 로그: `java.lang.OutOfMemoryError: GC overhead limit exceeded` + Docker 컨테이너 강제 종료

**원인 추적**

```
1단계: Spark UI 확인 → executor GC time 90%+ → 힙 부족 신호
2단계: docker stats → 컨테이너 메모리 한도(2GB)를 Spark executor가 초과 시도
3단계: spark-defaults.conf 확인 →
        spark.executor.memory = 1g (설정)
        spark.executor.memoryOverhead = 미설정 (기본값 384MB)
        → 실제 JVM 사용 = 1g + 384MB = 1.4GB, 여기에 Python worker + OS = 2GB 초과
```

**해결**
- Docker Compose 메모리 한도: 2GB → 4GB 상향
- JVM 가용 메모리 명시적 제한:

```properties
spark.executor.memory=1500m
spark.executor.memoryOverhead=512m
spark.driver.memory=1g
spark.driver.memoryOverhead=256m
```

- 추가: `spark.sql.shuffle.partitions=8` (기본 200 → 데이터 규모에 맞게 축소, 불필요한 셔플 오버헤드 제거)

**결과**: OOM Kill 재발 0건. 메모리 예산을 `executor + overhead + Python worker + OS` 네 항목으로 명시적으로 분리해 설정하는 기준 수립.

---

#### 2-3. AI 서버 구현 (Python FastAPI)

**LLM 배치 처리 최적화**
- Claude Haiku API 호출 시 10건 단위 배치 → API 호출 횟수 1/10 감소
- `asyncio.Semaphore(2)`로 동시 호출 수 제한 → OOM Kill 방지
- LLM 응답 불완전 포맷 대응 JSON 파싱 복구 로직 구현

**벡터 검색 파이프라인**
- 768차원 임베딩 생성 → pgvector IVFFlat 인덱스 저장
- 유사도 검색으로 관련 기술 아티클 추천

---

#### 2-4. 4단계 Fallback 추천 아키텍처

**설계 목표**: 캐시 히트율 최대화 + Cold Start 대응 + API 응답 < 50ms

```
1단계: Redis 캐시 HIT          → ~1ms    (최우선)
2단계: ALS 협업 필터링 재랭킹  → ~50ms
3단계: PostgreSQL 배열 교집합  → ~100ms  (관심 태그 기반)
4단계: Cold Start Top-20       → 기본 인기 아티클
```

- `@Async` 비동기 이벤트 처리로 이벤트 API 응답 < 50ms 유지

---

### 정량적 성과 요약

| 항목 | 수치 |
|---|---|
| 파이프라인 일 처리량 | **~1,800건, ~30MB** |
| 파이프라인 전체 사이클 | **~23~50분** |
| 추천 응답속도 (Redis 캐시 HIT) | **~1ms** |
| 이벤트 API 응답속도 | **< 50ms** |
| Spark Job 구현 (단독) | **4개** |
| Airflow DAG 구현 (단독) | **7개** |
| 인프라 장애 해결 | **8건, 재발 0건** |

---

## 3. SYNAPSE

### 프로젝트 개요

| 항목 | 내용 |
|---|---|
| **기간** | 2026.01 ~ 2026.02 (6주) |
| **팀 구성** | 6인 |
| **담당 역할** | 백엔드 / DB 설계 / CRDT 무한 루프 버그 추적·해결 / 코드 실행 세션 최적화 / Electron IPC 연동 |
| **기술 스택** | Spring Boot 3.x, Yjs CRDT, WebSocket, MongoDB, PostgreSQL, Redis, Electron |

**서비스 개요**
- 개발자 특화 실시간 협업 노트 플랫폼
- 동시 편집 충돌 없는 CRDT 기반 협업 + 코드 블록 실행 지원

---

### 핵심 기여

#### 3-1. 다중 DB 설계 (MongoDB + PostgreSQL)

**문제**
- 실시간 협업 문서(CRDT Delta, 가변 구조) + 사용자·권한·노트 메타(강한 일관성, JOIN) 를 단일 DB로 만족 불가

**설계 결정**

| DB | 저장 대상 | 선택 이유 |
|---|---|---|
| **PostgreSQL** | 사용자, 권한, 노트 메타데이터, 팀·멤버 관계 | 강한 일관성, JOIN, 트랜잭션 |
| **MongoDB** | Yjs CRDT Delta 문서, 노트 본문 스냅샷 | 스키마 유연성 — CRDT Delta는 연산마다 구조가 다름 |
| **Redis** | 실시간 협업 세션, 코드 실행 세션, 권한 캐시 | 초저지연, TTL 기반 만료 |

**MongoDB를 문서 저장소로 선택한 구체적 이유**
- Yjs Delta는 클라이언트마다 연산 구조가 달라 RDBMS 고정 스키마에 맞지 않음
- PostgreSQL JSONB도 검토했으나, Delta 크기가 가변적이고 부분 업데이트 빈도가 높아 MongoDB의 유연한 도큐먼트 모델이 더 적합

---

#### 3-2. CRDT 무한 reflow 버그 추적·해결

**증상**
- 특정 조건에서 편집 이벤트가 무한 루프 발생 → CPU 스파이크, 브라우저 탭 응답 없음
- 재현 조건: 두 클라이언트가 동시에 같은 위치 편집 후 한쪽이 오프라인 → 재연결 시 발생

**원인 추적**

```
클라이언트 A 편집 → Y-Doc Delta 생성 → WebSocket 서버로 전송
                                              ↓ 브로드캐스트
                                      클라이언트 A (자기 자신에게도 수신)
                                              ↓ Delta 적용
                                      Y-Doc 변경 감지 → 다시 Delta 생성
                                              ↓ 다시 전송 ... (무한 루프)
```

- 서버가 Delta를 브로드캐스트할 때 **발신자(origin)를 구분하지 않고 전체 구독자에게 전송**
- 발신자가 자신이 보낸 Delta를 다시 수신 → Y-Doc에 재적용 → 변경 이벤트 재발생 → 무한 reflow

**해결 — Yjs `origin` 태그로 발신자 구분**

Yjs의 `Y.Doc.on('update', (update, origin) => {...})` 콜백에서 `origin` 값으로 업데이트의 출처를 구분할 수 있다.

- 로컬 편집 시 `origin`에 클라이언트 고유 식별자(WebSocket connection ID) 태그
- 서버에서 브로드캐스트 수신 시 `origin`을 서버 식별자로 태그
- 클라이언트에서 `origin === 서버` 인 Delta는 Y-Doc 적용만 하고 **재전송하지 않음**

```
클라이언트 A 편집 → origin = 'clientA' → 서버 전송
                                              ↓ 브로드캐스트 (origin = 'server')
                              클라이언트 A 수신 → origin === 'server' → 재전송 안 함 ✅
                              클라이언트 B 수신 → origin === 'server' → Y-Doc 적용만 ✅
```

**결과**: 무한 reflow 완전 해소. 오프라인 재연결 시 동기화도 정상 동작.

---

#### 3-3. 코드 실행 세션 설계 결함 수정 — 실행 속도 30배 개선

**문제**
- 노트 내 코드 블록 실행 시마다 **새 Docker 컨테이너(세션)를 생성**하는 구조
- 컨테이너 생성 비용: 매 실행마다 ~3,000ms 소요 (이미지 pull 제외)
- 연속 실행 시 사용자가 매번 3초를 대기

**원인 — 설계 결함**
- 초기 설계에서 "실행 격리"를 위해 매번 새 컨테이너를 생성
- 실제로는 같은 노트의 연속 실행에서 **상태(변수, 임포트)를 유지해야** 하는 요구사항이 있었으나 반영 안 됨
- 결과적으로 격리도 안 되고(노트 단위로 공유되어야 함) 느리기까지 한 구조

**해결 — 세션 재사용 구조로 전환**
- 노트별로 Docker 컨테이너(실행 세션)를 **최초 1회 생성 후 Redis에 세션 ID 등록**
- 이후 같은 노트의 코드 실행 요청은 기존 컨테이너에 코드만 주입해 실행
- 일정 시간 미사용 시 TTL로 컨테이너 자동 종료 및 세션 제거

```
기존: 실행 요청 → 컨테이너 생성(~3,000ms) → 코드 실행 → 컨테이너 종료
변경: 실행 요청 → Redis에서 세션 조회
        세션 있음 → 기존 컨테이너에 코드 주입 → 실행 (~100ms)
        세션 없음 → 컨테이너 생성 → Redis 등록 → 실행
```

**결과**

| 항목 | Before | After |
|---|---|---|
| 첫 실행 (세션 없음) | ~3,000ms | ~3,000ms (동일) |
| 연속 실행 (세션 있음) | ~3,000ms (매번 생성) | **~100ms (약 30배 개선)** |
| 상태 유지 | ❌ 매번 초기화 | ✅ 변수·임포트 유지 |

---

#### 3-4. Electron IPC — 로컬 Docker 실행 연동

**구현 배경**
- 코드 실행 기능이 서버 사이드 Docker에만 의존하면 네트워크 지연·서버 비용 발생
- 로컬에 Docker가 설치된 환경에서는 로컬에서 직접 실행하는 것이 더 빠르고 안전

**구현 내용**
- Electron Main Process에서 로컬 Docker 데몬 연결 가능 여부 감지
- Renderer Process(웹 UI)에서 코드 실행 요청 시 IPC 채널을 통해 Main Process로 전달
- Main Process가 로컬 Docker 컨테이너에 코드 주입·실행 후 결과를 IPC로 반환

```
Renderer (웹 UI)
    ↓ ipcRenderer.invoke('docker:exec', { code, lang })
Main Process
    ↓ Docker SDK → 로컬 컨테이너 실행
    ↓ ipcMain.handle('docker:exec', ...) → 결과 반환
Renderer
    ↓ 실행 결과 출력
```

- 로컬 Docker 미설치 환경에서는 자동으로 서버 사이드 실행으로 폴백

---

### 정량적 성과 요약

| 항목 | 수치 |
|---|---|
| 코드 연속 실행 속도 개선 | ~3,000ms → **~100ms (약 30배)** |
| CRDT 무한 reflow | origin 태그로 **완전 해소** |
| DB 구성 | PostgreSQL + MongoDB + Redis **3DB 하이브리드** |
| Electron IPC | 로컬 Docker 실행 연동, **서버 폴백 자동 전환** |

---

---

*GitHub: https://github.com/ljy0221*
*이메일: jaeyeong221@gmail.com*
