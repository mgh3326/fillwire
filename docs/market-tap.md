# KIS REST 분봉 shadow 폴러 (`market-tap`)

hk 6344 / 설계 `docs/design/6321-go-quote-collector.md` §3.1·§8. `market-tap` 은 KIS 국내
(`FHKST03010200`, 시장 `J`=KRX·`NX`=NTX)와 해외(`HHDFS76950200`) **분봉 GET 만** 호출하는
shadow 폴러입니다. 결과는 JSONL 파일과 Redis Stream 에만 쓰며 research 표에 ingest 하지
않습니다(단계 0). 주문·정정·취소·원장 경로와 KIS 토큰 발급 경로를 링크하지 않습니다.

기본값은 **비활성화** 입니다. `-enabled` 없이 실행하면 아무 요청도 보내지 않고 0으로
끝납니다.

## 실행

```sh
market-tap \
  -enabled \
  -targets /etc/fillwire/market-tap-targets.json \
  -calendar /etc/fillwire/market-tap-calendar.json \
  -out /var/lib/fillwire/market-tap/bars.jsonl \
  -once
```

| 플래그 | 기본값 | 의미 |
|---|---|---|
| `-enabled` | off | 켜지 않으면 아무 것도 하지 않음 |
| `-targets` | — | auto_trader `build_targets` 결과를 옮긴 대상 목록 JSON |
| `-calendar` | — | 거래일 달력 JSON(휴장일·미국 조기 마감) |
| `-out` | — | JSONL 추가 기록 경로 |
| `-redis` | — | Redis Stream 기록 URL (`minute:kis:<symbol>`, `MAXLEN ~ 100000`) |
| `-once` | false | 한 번만 폴링(없으면 `-interval`, 기본 5분) |
| `-workers` | 4 | 동시 분봉 GET 수 |
| `-host` | KIS 실전 | REST 호스트. 모의는 `https://openapivts.koreainvestment.com:29443` |
| `-provider` / `-account-ref` | `kis` / `live` | `internal/ratebudget` 키. 같은 브로커의 두 번째 키는 기동 거부(exit 78) |
| `-rate` / `-burst` | 20 / 20 | provider 전역 토큰 버킷(KIS 실전 한도) |
| `-endpoint-rate` / `-endpoint-burst` | 20 / 20 | endpoint 버킷 |
| `-research-share` | 0.25 | 전역 버킷 중 이 프로세스의 고정 몫 |
| `-token-source` | `redis` | `redis`(공유 캐시 읽기) 또는 `gatewayd-fetch`(gatewayd `/fetch`) |
| `-token-key` | `kis:access_token` | Redis 토큰 키(모의는 `kis_mock:<host>:<app-key 지문>:access_token`) |
| `-gatewayd-url` / `-gatewayd-token-env` / `-gatewayd-provider` | — | `gatewayd-fetch` 설정. provider는 `kis-live`/`kis-mock` |
| `-app-key-env` / `-app-secret-env` | `KIS_APP_KEY` / `KIS_APP_SECRET` | KIS REST 헤더에 쓸 자격증명의 환경변수 이름. 값은 로그에 남지 않음 |

종료 코드: `0` 정상, `1` 실행 실패, `2` 명령줄 오류, `78` 설정 거부. KIS 자격증명
환경변수가 비어 있으면 기동을 거부합니다.

## 토큰

`-token-source redis` 는 auto_trader/gatewayd 가 Redis 에 게시한 access token 을
`internal/kisread` 계약으로 **읽기만** 합니다. `-token-source gatewayd-fetch` 는 gatewayd 의
`POST /v1/tokens/{provider}/fetch` 만 호출하고 토큰을 메모리에 둡니다. 어느 쪽도 발급·갱신·
잠금·쓰기를 하지 않습니다. 만료 응답(`EGW00123`/`EGW00121`)을 받으면 토큰 원본을 한 번 다시
읽고 GET 을 한 번만 재시도합니다.

## 달력

Go 는 auto_trader 의 `exchange_calendars` 데이터를 재현하지 않습니다. `-calendar` 파일에
같은 달력의 휴장일과 미국 조기 마감을 넣습니다. 파일에 없는 날은 주말이 아니어도 열린
세션으로 추정하지 않습니다(fail-closed).

```json
{
  "kr_closed": ["2026-12-25"],
  "us_closed": ["2026-11-26"],
  "us_early_close": {"2026-11-27": "13:00"}
}
```

알 수 없는 키, 잘못된 날짜·시각은 기동 거부(exit 78)입니다.

## 대상 목록

Go 는 종목을 고르지 않습니다. auto_trader 의 `build_targets` 결과를 다음 모양으로 옮깁니다.

```json
{"targets": [
  {"market": "kr", "symbol": "005930", "venue": "KRX"},
  {"market": "kr", "symbol": "005930", "venue": "NTX"},
  {"market": "us", "symbol": "NVDA", "venue": "NASD"}
]}
```

KR venue 는 `KRX`/`NTX`, US venue 는 `NASD`/`NYSE`/`AMEX` 만 허용합니다.

## 행 규칙 (Python 과 동일)

- 봉은 거래소 현지 시각의 **시작** 분으로 표기되고, 형성 중 봉도 응답에 포함됩니다.
  `봉 시작 + 1분 + 10초 <= 요청 직전 시각` 일 때만 기록합니다.
- KR `value` 는 일 누적 거래대금입니다. 직전 같은 KST 날짜 행과의 차이, 또는 venue 첫
  분(KRX 09:00, NTX 08:00)의 누적값 자체일 때만 거래대금을 기록하고, 그 밖에는 `null`
  입니다. 누적값이 감소한 행, 그 다음 행, 값을 읽지 못한 행, 버려진 행 다음 행도
  `null` 입니다. 행은 버리지 않습니다.
- US `value` 는 분당 값이므로 그대로 기록합니다.
- `session_segment` 는 `KRX_REGULAR`, `KRX_AFTER`, `NXT_PRE`, `NXT_OVERLAP`,
  `NXT_POST`, `US_PRE`, `US_REGULAR`, `US_POST`, 또는 `UNKNOWN` 입니다. 달력이 확인하지
  못한 날은 `UNKNOWN` 이며 추정하지 않습니다.

행 한 줄의 키 순서는
`time_utc, session_date, symbol, venue, session_segment, source, open, high, low, close,
volume, value` 이고, 10진 필드는 정확한 문자열, `value` 는 증명되지 않으면 `null` 입니다.
이 직렬화는 auto_trader 의 `bars_from_frame`·`session_segment_of` 출력과 바이트 단위로
비교합니다(`internal/marketminute/testdata/bars/`). Redis 에는 `null` 필드가 없으므로
`value` 키를 생략합니다.

## ratebudget

`internal/ratebudget` 의 계층 버킷을 씁니다. 요청마다 provider 전역 버킷과
`TR_ID|path` endpoint 버킷을 모두 통과해야 하고, 거절은 `rate_limited`/`circuit_open`/
`budget_cut` 중 하나로 보고한 뒤 `RetryAfter` 만큼 기다렸다가 다시 시도합니다. 429 또는
KIS `EGW00201` 은 endpoint 버킷을 닫고, 5xx·전송 실패는 회로 실패로 기록합니다. 별도의
limiter 는 두지 않습니다.

## 검증

```sh
go test ./internal/marketminute ./cmd/market-tap
```

`cmd/market-tap/noorder_test.go` 는 링크된 패키지(`go-kis` 주문 패키지, `internal/exec*`,
`internal/kislive`, `internal/kismockedge`, `internal/gatewayd`, `internal/reader`)와
바이너리의 주문 심볼·주문 경로/TR 을 검사합니다. `internal/marketminute` 골든 테스트는
Python 함수가 만든 행과 세션 라벨을 바이트 단위로 비교합니다.
