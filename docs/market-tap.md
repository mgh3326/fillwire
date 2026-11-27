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
  -token-redis rediss://redis.example.invalid:6380/0 \
  -once
```

| 플래그 | 기본값 | 의미 |
|---|---|---|
| `-enabled` | off | 켜지 않으면 아무 것도 하지 않음 |
| `-targets` | — | auto_trader `build_targets` 결과를 옮긴 대상 목록 JSON |
| `-calendar` | — | 거래일 달력 JSON(휴장일·미국 조기 마감) |
| `-out` | — | JSONL 추가 기록 경로 |
| `-redis` | — | Redis Stream 기록 URL (`minute:kis:<venue>:<symbol>`, `MAXLEN ~ 100000`) |
| `-once` | false | 한 번만 폴링(없으면 `-interval`, 기본 5분) |
| `-us-day-market` | off | KIS 미국 주간거래 분봉 원천을 추가([아래](#미국-주간거래us_day-원천-기본-off)). 달력에 `us_day_market` 창이 없으면 exit 78 |
| `-workers` | 4 | 동시 분봉 GET 수 |
| `-host` | KIS 실전 | REST 호스트. `https://` 또는 loopback `http://` 만 허용(그 외 exit 78). 모의는 `https://openapivts.koreainvestment.com:29443` |
| `-provider` / `-account-ref` | `kis` / `live` | `internal/ratebudget` 키. 같은 브로커의 두 번째 키는 기동 거부(exit 78) |
| `-rate` / `-burst` | 20 / 20 | provider 전역 토큰 버킷(KIS 실전 한도) |
| `-endpoint-rate` / `-endpoint-burst` | 20 / 20 | endpoint 버킷 |
| `-research-share` | 0.25 | 전역 버킷 중 이 프로세스의 고정 몫 |
| `-token-source` | `redis` | `redis`(공유 캐시 읽기) 또는 `gatewayd-fetch`(gatewayd `/fetch`) |
| `-token-key` | `kis:access_token` | Redis 토큰 키(모의는 `kis_mock:<host>:<app-key 지문>:access_token`) |
| `-token-redis` | — | JSONL 만 쓸 때 토큰을 읽을 Redis URL. 비우면 `-redis` 를 씀 |
| `-gatewayd-url` / `-gatewayd-token-env` / `-gatewayd-provider` | — | `gatewayd-fetch` 설정. provider는 `kis-live`/`kis-mock` |
| `-app-key-env` / `-app-secret-env` | `KIS_APP_KEY` / `KIS_APP_SECRET` | KIS REST 헤더에 쓸 자격증명의 환경변수 이름. 값은 로그에 남지 않음 |

종료 코드: `0` 정상, `1` 실행 실패, `2` 명령줄 오류, `78` 설정 거부. KIS 자격증명
환경변수가 비어 있으면 기동을 거부합니다.

## 토큰

`-token-source redis` 는 auto_trader/gatewayd 가 Redis 에 게시한 access token 을
`internal/kisread` 계약으로 **읽기만** 합니다. JSONL 만 쓰는 실행은 `-token-redis` 로 토큰 캐시를 지정합니다(이 경우 Redis Stream sink 는 꺼집니다). `-token-source gatewayd-fetch` 는 gatewayd 의
`POST /v1/tokens/{provider}/fetch` 만 호출하고 토큰을 메모리에 둡니다. 어느 쪽도 발급·갱신·
잠금·쓰기를 하지 않습니다. 만료 응답(`EGW00123`/`EGW00121`)을 받으면 토큰 원본을 한 번 다시
읽고 GET 을 한 번만 재시도합니다.

## 달력

Go 는 auto_trader 의 `exchange_calendars` 데이터를 재현하지 않습니다. `-calendar` 파일에
같은 달력의 **휴장일 전부**와 미국 조기 마감을 넣습니다. 주말은 자동 휴장이고, 그 밖의
평일은 파일에 없으면 열린 세션으로 봅니다. 따라서 파일에서 빠진 휴장일은 `UNKNOWN` 이
아니라 세션으로 표기되므로, 운영자는 auto_trader 달력의 휴장일을 빠짐없이 옮겨야 합니다.

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

KR venue 는 `KRX`/`NTX`, US venue 는 `NASD`/`NYSE`/`AMEX` 만 허용합니다. 주간거래 코드
(`BAQ`/`BAY`/`BAA`)는 대상 목록에 넣지 않습니다(넣으면 exit 78).

## 미국 주간거래(US_DAY) 원천 (기본 off)

hk 6347 / 설계 §4.4. `-us-day-market` 을 켜면 대상 목록의 US 종목마다 주간거래 분봉을
한 번 더 읽습니다.

### 공식 근거

KIS 공식 샘플 저장소 `koreainvestment/open-trading-api`, 커밋
`b0a0d33b7e5f863a8f3c6790f50adb840829c806` (2026-11-27 확인).

| 확인한 것 | 값 | URL |
|---|---|---|
| TR·경로 | `HHDFS76950200` `/uapi/overseas-price/v1/quotations/inquire-time-itemchartprice` (정규장 해외 분봉과 같음) | [inquire_time_itemchartprice.py](https://github.com/koreainvestment/open-trading-api/blob/b0a0d33b7e5f863a8f3c6790f50adb840829c806/examples_llm/overseas_stock/inquire_time_itemchartprice/inquire_time_itemchartprice.py#L97) |
| 주간거래 거래소 코드 `EXCD` | `BAY` 뉴욕, `BAQ` 나스닥, `BAA` 아멕스. 주간거래는 최대 1일치 분봉 | [같은 파일 51행](https://github.com/koreainvestment/open-trading-api/blob/b0a0d33b7e5f863a8f3c6790f50adb840829c806/examples_llm/overseas_stock/inquire_time_itemchartprice/inquire_time_itemchartprice.py#L51) |
| 한국 일자·시각 필드 | `kymd`·`khms` | [chk_inquire_time_itemchartprice.py](https://github.com/koreainvestment/open-trading-api/blob/b0a0d33b7e5f863a8f3c6790f50adb840829c806/examples_llm/overseas_stock/inquire_time_itemchartprice/chk_inquire_time_itemchartprice.py#L35-L38) |

확인하지 못한 것:

- **실제 API 응답은 측정하지 않았습니다.** 위 값은 공식 샘플에서 확인한 것이며, 이
  저장소에서 KIS 를 호출해 본 적이 없습니다. 켠 뒤 첫 세션에서 응답을 확인해야 합니다.
- 주간거래 봉의 라벨이 **시작** 분이라는 것은 같은 TR 의 정규장 규칙에서 가정한
  것입니다.
- 주간거래 시간은 공식 샘플끼리 다릅니다
  ([10:00~16:00](https://github.com/koreainvestment/open-trading-api/blob/b0a0d33b7e5f863a8f3c6790f50adb840829c806/examples_user/overseas_stock/overseas_stock_functions_ws.py#L218),
  [10:00~18:00](https://github.com/koreainvestment/open-trading-api/blob/b0a0d33b7e5f863a8f3c6790f50adb840829c806/legacy/Sample01/kis_ovrseastk.py#L656), KST).
  그래서 코드에 시간을 박지 않고 달력 파일의 창을 씁니다.

### 달력의 주간거래 창

```json
{
  "us_closed": ["2026-11-26"],
  "us_day_market": [
    {"start": "2026-11-30T10:00:00+09:00", "end": "2026-11-30T16:00:00+09:00"}
  ]
}
```

위 시각은 형식 예시입니다. auto_trader 가 쓰는 Toss `market-calendar/US` 의 `dayMarket`
창(hk 6322)을 옮깁니다. 시작 포함·끝 제외이고 RFC3339(오프셋 필수), 창 하나는 24시간
이하, 겹치면 안 됩니다.

주간거래의 열림·닫힘은 **이 창으로만** 정합니다. `us_closed`·조기 마감·정규장 마감은
주간거래에 적용하지 않습니다. 파일은 기동 때 한 번 읽으므로 창을 갱신하면 다시
시작해야 합니다.

폴 보고의 `us_day_market`:

| 필드 | 값 |
|---|---|
| `state` | `open`(창 안), `closed`(창 사이), `uncovered`(파일의 첫 창 이전·마지막 창 이후: 닫혔는지 알 수 없음. 파일을 갱신하세요) |
| `targets` | 대상 목록에서 만든 주간거래 대상 수 |
| `read` | 이번 폴에서 읽었는지. 창 안이거나, 창이 끝난 뒤 `max(15분, -interval × 2)` 이내 |
| `rows` | 이번 폴에서 쓴 주간거래 행 수 |

### 행

- `venue` 는 `BAQ`/`BAY`/`BAA` 이고 `source` 는 `KIS` 입니다. 정규장 venue 봉과 키·Redis
  Stream(`minute:kis:BAQ:<symbol>`)을 공유하지 않습니다.
- `time_utc` 는 `kymd`·`khms`(KST)에서, `session_date` 는 기존 20:00 ET 롤오버 규칙으로
  정합니다. 창 안에서 시작한 봉은 `US_DAY`, 밖은 `UNKNOWN` 입니다.
- auto_trader 의 분봉 ingest 와 `research.us_candles_1m` 은 이 venue 를 아직 받지
  않습니다. shadow 출력 전용입니다.

### 한도와 실패

주간거래 읽기는 정규장 대상을 다 쓴 뒤에 돌고, provider 전역 버킷을 함께 씁니다.
endpoint 버킷과 회로는 따로(`HHDFS76950200.US_DAY|…`)이며 `Isolated` 라서 provider 회로
판정에 들어가지 않습니다.

- 패스는 먼저 대상 하나를 단독으로 읽어 반개방 회로의 probe 로 쓰고, 나머지는 그 결과를
  본 뒤 병렬로 읽습니다. probe 대상은 패스마다 목록 순으로 돌아가므로, 항상 실패하는
  종목 하나가 probe 를 계속 붙잡지 않습니다.
- 회로가 아직 열린 창(남은 대기 시간이 있는 `circuit_open`)인 대상은 기다리지 않고
  `circuit_open` 으로 보고한 뒤 건너뜁니다. 대기 시간이 0 인 거절은 건너뛰지 않고 기다렸다가
  다시 시도합니다.
- 주간거래 패스는 `DayMarketBudget`(기본 30초) 안에 끝냅니다. 시작하지 못하거나 대기가 예산을
  넘는 대상은 `day_budget` 으로 보고하며, 정규장 다음 폴이 그만큼 밀리지 않습니다.
- 정규장 행은 주간거래 패스 전과 종료(오류 포함) 시 sink 를 flush 해서 버퍼에 남지 않습니다.

끄려면 `-us-day-market` 을 뺍니다. `us_day_market` 키를 모르는 이전 이미지는 그 키가 있는
달력을 거부하므로(exit 78), 이미지를 되돌릴 때는 달력의 키도 지웁니다.

## 행 규칙 (Python 과 동일)

- 봉은 거래소 현지 시각(주간거래 venue 는 KST)의 **시작** 분으로 표기되고, 형성 중 봉도 응답에 포함됩니다.
  `봉 시작 + 1분 + 10초 <= 요청 직전 시각` 일 때만 기록합니다.
- KR `value` 는 일 누적 거래대금입니다. 직전 같은 KST 날짜 행과의 차이, 또는 venue 첫
  분(KRX 09:00, NTX 08:00)의 누적값 자체일 때만 거래대금을 기록하고, 그 밖에는 `null`
  입니다. 누적값이 감소한 행, 그 다음 행, 값을 읽지 못한 행, 버려진 행 다음 행도
  `null` 입니다. 행은 버리지 않습니다.
- US `value` 는 분당 값이므로 그대로 기록합니다.
- 이미 기록한 봉(대상별 마지막 `time_utc` 워터마크 이하)은 다음 폴링에서 다시 쓰지
않습니다. KIS 가 같은 꼬리 페이지를 돌려주므로 이 중복 제거가 없으면 JSONL 이 무한히
커지고 분당 거래대금이 두 번 집계됩니다.

`session_segment` 는 `KRX_REGULAR`, `KRX_AFTER`, `NXT_PRE`, `NXT_OVERLAP`,
  `NXT_POST`, `US_PRE`, `US_REGULAR`, `US_POST`, `US_DAY`(주간거래 원천), 또는 `UNKNOWN` 입니다. 주말·휴장일로 표시된
  날과 창 밖 시간대는 `UNKNOWN` 입니다.

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
limiter 는 두지 않습니다. 주간거래 대상의 `circuit_open` 은 재시도하지 않습니다(위 절).

## 검증

```sh
go test ./internal/marketminute ./internal/markettapcmd ./internal/ratebudget ./cmd/market-tap
```

`cmd/market-tap/noorder_test.go` 는 링크된 패키지(`go-kis` 주문 패키지, `internal/exec*`,
`internal/kislive`, `internal/kismockedge`, `internal/gatewayd`, `internal/reader`)와
바이너리의 주문 심볼·주문 경로/TR 을 검사합니다. `internal/marketminute` 골든 테스트는
Python 함수가 만든 행과 세션 라벨을 바이트 단위로 비교합니다.
