# 포트폴리오 — Butterfly CRM 캠페인 플랫폼

> 이재영 | Backend Engineer
> 작성일: 2026-05-22

---

## 프로젝트 개요

**Butterfly**는 멀티테넌트 마케팅 자동화 SaaS입니다.
고객 행동 이벤트 수집 → 세그먼트 계산 → 캠페인 실행 → 성과 분석까지 하나의 플랫폼으로 묶은 CRM 운영 엔진입니다.

| 항목 | 내용 |
|---|---|
| **기간** | 2026-04 ~ 2026-05 (6주) |
| **팀 구성** | 6명 (도메인별 역할 분담) |
| **담당 역할** | 역할 6 — 분석/집계 파이프라인 · 발송 추적 · E2E 성능 검증 · 관측성 인프라 |
| **기술 스택** | Java 21, Spring Boot 3.5, MySQL 8, Kafka, Redis, Grafana/Loki/Prometheus, K3s |

---

## 문제 해결 중심 기여

---

### 1. 도메인 경계를 지키면서 실시간 집계를 달성한 방법

**문제**: analytics 모듈이 발송 결과를 알아야 집계가 가능합니다. 그런데 `butterfly-reporting`이 `butterfly-notification`의 DB를 직접 조회하면 ADR-0014(모듈 간 직접 의존 차단)를 위반합니다. 팀에서는 "그냥 직접 조회하면 안 되나?"라는 의견도 있었습니다.

**분석**: 직접 조회 허용 시 두 모듈의 스키마가 결합됩니다. notification이 테이블 구조를 바꾸면 analytics가 동시에 깨지고, 배포 순서 제약이 생깁니다. 장기적으로 "모듈이 2개인 모놀리스"가 됩니다.

**해결**: notification 워커가 발송 완료 시 `DeliveryTrackingEvent`를 Kafka에 발행 → analytics 컨슈머가 수신해 집계 테이블 UPSERT. 두 도메인은 Kafka 토픽 계약으로만 연결됩니다.

```
butterfly-worker
  └─ DeliveryResultPersister → DeliveryTrackingProducer
                                      ↓ [delivery-tracking topic]
                               butterfly-reporting
                                  └─ DeliveryTrackingConsumer → CampaignStatsHourlyRepository.upsert()
```

**결과**: 도메인 경계 유지 + 5분 주기 실시간 집계 달성. notification 스키마가 바뀌어도 analytics 코드는 변경 불필요.

**설계 결정 기록**: ADR-0014 (모듈 간 직접 의존 차단), ADR-0007 (2단 집계 구조)

---

### 2. 집계 쿼리 풀 테이블 스캔 — 인덱스 5개로 3곳을 동시에 해결

**문제**: 5분마다 실행되는 `aggregateHourly()` 배치가 실제 운영 규모에서 얼마나 걸리는지 궁금했습니다. 103만 건 데이터를 기준으로 EXPLAIN을 실행했습니다.

```sql
EXPLAIN SELECT project_id, campaign_id, channel, COUNT(*)
FROM notification_logs
WHERE created_at >= NOW() - INTERVAL 1 HOUR
GROUP BY project_id, campaign_id, channel;
-- type: ALL | rows: 1,030,302 | 실행 시간: 3,743ms
```

`type: ALL` — 100만 건 전체를 읽고 있었습니다. 같은 방식으로 발송 수신자 조회와 세그먼트 만료 배치도 점검했고, 두 곳도 동일하게 풀 스캔이었습니다.

**분석**:
- `aggregateHourly`: `created_at` 범위 필터 인덱스 없음 → 전체 스캔
- PUSH 수신자 조회: `users.push_opt_in` 필터 인덱스 없음 → `project_id`로 뽑은 5만 건 중 10%만 통과, 90% 버림
- 세그먼트 만료 배치: `segment_members.expires_at` 인덱스 없음 → 만료 행 찾으려고 전체 스캔

**해결**: 인덱스 5개 추가, Flyway 마이그레이션으로 관리.

```sql
ALTER TABLE notification_logs
  ADD INDEX idx_notification_logs_time_campaign (created_at, campaign_id, channel);
ALTER TABLE users ADD INDEX idx_users_project_push_opt_in (project_id, push_opt_in);
ALTER TABLE users ADD INDEX idx_users_project_email (project_id, email(100));
ALTER TABLE users ADD INDEX idx_users_project_phone (project_id, phone);
ALTER TABLE segment_members ADD INDEX idx_segment_members_expires_at (expires_at);
```

**결과 (103만 건 실측)**:

| 쿼리 | Before | After | 개선 |
|---|---|---|---|
| `aggregateHourly` (집계) | 3,743ms | 2.8ms | **1,314배** |
| PUSH 수신자 조회 | 27.4ms | 6.2ms | **4.4배** |
| 세그먼트 만료 배치 | 1.8ms | 0.75ms | **2.4배** |

수정한 코드 0줄. EXPLAIN으로 원인을 확인하고 인덱스만 추가했습니다.

---

### 3. 설정은 있는데 작동 안 하는 Hibernate 배치 — 원인을 찾기까지

**문제**: LT-05 N=30,000 부하 테스트에서 `notification-send-result` 컨슈머의 TPS가 **16.2/s 상수**로 고정됐습니다. `application.yml`에는 분명히 `batch_size: 1000`이 설정돼 있었습니다.

```yaml
spring.jpa.properties.hibernate.jdbc.batch_size: 1000
spring.jpa.properties.hibernate.order_inserts: true
```

**분석**: `NotificationLog` 엔티티를 확인했습니다.

```java
@Id
@GeneratedValue(strategy = GenerationType.IDENTITY)  // ← 여기
private Long id;
```

`GenerationType.IDENTITY`는 MySQL `AUTO_INCREMENT`입니다. INSERT 후 DB가 생성한 id를 즉시 읽어야 하므로, **Hibernate가 JDBC 배치를 자동으로 비활성화**합니다. `saveAll()`을 사용해도 내부에서 1건씩 INSERT합니다. `batch_size` 설정이 이 테이블에 대해서는 의미가 없습니다.

동시에 컨슈머 구조 문제도 있었습니다. 레코드 1건씩 수신, 1건씩 트랜잭션 커밋 — N=30,000이면 커밋이 30,000번입니다.

**해결**: JPA를 우회해 `NamedParameterJdbcTemplate.batchUpdate()`로 직접 bulk INSERT. Kafka 컨슈머를 배치 리스너로 전환해 poll당 최대 500건을 한 트랜잭션으로 묶었습니다.

```java
// Before: 레코드 1건씩
public void consume(NotificationSendResultPayload payload) {
    notificationLogRepository.save(log);  // 1 INSERT, 1 commit
}

// After: poll 배치 단위
public void consume(List<NotificationSendResultPayload> payloads) {
    bulkRepository.bulkInsert(valid);  // batchUpdate, 1 commit
}
```

`INSERT IGNORE`를 써서 `message_key` 중복 시 해당 row만 건너뛰고 배치 전체가 롤백되지 않도록 했습니다.

| 항목 | Before | After |
|---|---|---|
| INSERT 방식 | JPA save() — IDENTITY로 batch 불가 | JDBC batchUpdate() — JPA 우회 |
| 트랜잭션 커밋 | 레코드당 1회 | poll 배치당 1회 |
| N=500 기준 DB 왕복 | 500회 | 1회 |
| **TPS (N=30,000, K3s 실측)** | **16.2/s** | **150.0/s (+826%, 9.3×)** |

---

### 4. 멱등성 키가 100% NULL — 상류가 키를 만들지 않으면 하류 방어는 무의미하다

**문제**: LT-05 완주 후 `notification_logs.message_key` 전수 조사 결과 100% NULL이었습니다. UNIQUE 제약이 걸려 있지만, MySQL은 NULL을 중복으로 보지 않습니다. 워커가 재기동되면 같은 발송이 여러 번 INSERT됩니다.

**분석**: 파이프라인을 역추적했습니다.

```
DeliveryCommand (record, 8필드)     ← messageKey 필드 없음
    ↓
DeliveryServiceImpl                 ← messageKey 참조 없음
    ↓
DeliveryResultPersister             ← NotificationLog.builder()에 .messageKey() 없음
```

`butterfly-reporting`의 `RedisMessageKeyGuard`는 완벽하게 구현돼 있었습니다. 그런데 상류 `butterfly-notification`이 키를 생성하지 않으니 하류의 중복 차단은 모두 무의미한 상태였습니다. 두 모듈을 담당하는 팀이 달랐기 때문에 발생한 구조적 불일치입니다.

**해결 방향**: `DeliveryCommand`에 `messageKey` 필드 추가 → `DeliveryServiceImpl`에서 `campaignId:userId:executionId` 조합으로 생성 → `NotificationLog`에 세팅.

**교훈**: 멱등성 키 계약은 스키마 설계 시점에 `NOT NULL`로 강제해야 합니다. 상류가 키를 안 만들면 하류의 방어 코드는 실행조차 안 됩니다. "기능이 되는 것"과 "운영에서 안전한 것"은 다릅니다.

---

### 5. 재현 가능한 성능 검증 환경 — 시나리오 3개, 결함 2개 발견

**문제**: 팀에 성능 검증 환경이 없었습니다. "잘 되는 것 같다"는 수준의 검증으로는 운영 이슈를 사전에 발견하기 어렵습니다.

**해결**: ADR-0017에 SLA를 명문화하고, k6 시나리오 3개와 시드 SQL을 설계했습니다.

- `FakeFcmChannelAdapter`: `@ConditionalOnProperty`로 실제 FCM과 상호 배제. 50ms 고정 지연으로 외부 의존 격리 후 시스템 자체 병목만 측정
- `seed-bulk-dispatch-fixtures.sql`: users 10,000 + devices 10,000 + segments/campaigns 자동 적재
- `lt05-kafka-inject.sh N CAMPAIGN_ID USER_START`: N건 `DeliveryCommand` Kafka 주입 + 완료 polling
- `lt01-event-ingest.js`: 이벤트 수집 API SLA 검증 (100 req/s × 5min)
- `lt03-aggregation.js`: hourly 집계 + daily 롤업 latency 검증

**결과 (K3s 클러스터 실측)**:

| 시나리오 | 측정값 | 기준 |
|---|---|---|
| LT-01: 이벤트 수집 API | p95 **20.7ms**, 실패율 **0%**, 30,000건 | p99 < 200ms (ADR-0017) |
| LT-03: hourly 집계 | **10ms** | < 5,000ms |
| LT-03: daily 롤업 | **4ms** | < 30,000ms |
| LT-05: Kafka 배치 컨슈머 (Before) | TPS **16.2/s** | — |
| LT-05: Kafka 배치 컨슈머 (After) | TPS **150.0/s** (+826%) | — |

이 환경이 있었기 때문에 TPS 상수 고정 → IDENTITY 키 문제(기여 3번), message_key 100% NULL(기여 4번)을 발견할 수 있었습니다.

---

### 6. Grafana Alloy를 도입하지 않은 이유 (ADR-0018)

**상황**: Promtail이 EOL(2026-02-28)을 맞았습니다. 공식 후속은 Grafana Alloy입니다. "마이그레이션해야 하지 않나?"라는 논의가 있었습니다.

**분석**: Alloy는 River 문법 기반의 새 설정 체계가 필요합니다. 학습 + 마이그레이션 + 검증에 최소 1.5~2일 소요. 남은 개발 기간이 14일인 시점에서 이 비용은 수용하기 어렵습니다. EOL이지만 Promtail은 여전히 동작하고, 실제 기능 차이는 우리 사용 범위에서 미미합니다.

**결정**: Promtail 유지, Alloy 마이그레이션은 deferred. trade-off를 ADR-0018에 명문화.

**추가 결정 — cardinality 금지 규칙**: `userId`, `projectId`, `campaignId`를 Loki 라벨 / Prometheus 메트릭 라벨로 사용하는 것을 BLOCKER로 지정. 레이블 인덱스가 폭발하면 Loki 전체가 느려집니다.

---

## 정량적 성과 요약

| 항목 | 수치 |
|---|---|
| `aggregateHourly` 쿼리 개선 | 3,743ms → **2.8ms** (1,314배) |
| PUSH 수신자 조회 개선 | 27.4ms → **6.2ms** (4.4배) |
| 세그먼트 만료 배치 개선 | 1.8ms → **0.75ms** (2.4배) |
| Kafka 배치 컨슈머 전환 (K3s 실측) | **TPS 16.2/s → 150.0/s (+826%, 9.3×)** |
| 부하 테스트 완주 | N=30,000, Before 16.2/s → After **150.0/s** |
| **이벤트 수집 API SLA 검증 (LT-01, K3s 실측)** | **100 req/s × 5min, p95 20.7ms, 실패율 0%** (ADR-0017 p99 < 200ms 통과) |
| **집계 워커 latency 검증 (LT-03, K3s 실측)** | **hourly 10ms / daily 4ms** (임계치 대비 500×~7,500× 마진) |
| ADR 작성/기여 | **19건** |
| 발견한 운영 결함 | **2건** (message_key NULL, 과다 로깅) |

---

## 성장 포인트

| 영역 | Before | After |
|---|---|---|
| **Kafka** | 기본 producer/consumer 수준 | at-least-once 시맨틱, 멱등성 키 설계, IDENTITY 키가 배치를 비활성화하는 원인까지 이해 |
| **성능 측정** | 막연한 "빠르다/느리다" | EXPLAIN 실행 계획 분석, TPS 측정, 병목 원인 추적 |
| **아키텍처** | 단일 모듈 중심 | 도메인 경계 설계, SPI 포트 패턴, Kafka 토픽 계약으로 의존 분리 |
| **관측성** | 로그 파일 수동 확인 | Loki LogQL + Prometheus PromQL + requestId 3축 연계 진단 |
| **의사결정** | 기술 선택 근거 없음 | ADR로 맥락·trade-off·deferred 이유를 명문화 |

---

## 링크

- **GitLab**: [S14P31B101](https://lab.ssafy.com/s14-final/S14P31B101)
- **블로그 시리즈**: [docs/portfolio/blog/](./blog/)
- **KPI 정의서**: [backend/docs/operations/kpi-and-attribution.md](../backend/docs/operations/kpi-and-attribution.md)
- **ADR 목록**: [backend/docs/adr/README.md](../backend/docs/adr/README.md)
