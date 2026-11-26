# pi에서 Fillwire serve와 분봉 수집기 전환

`fillwire serve`와 `fillwire market-tap`을 GHCR의 같은 다중 아키텍처 이미지로
실행합니다. 배포 사본은 설계 세션이 검토한 뒤 pi에 복사합니다.

## 사본 준비와 이미지 교체

- [serve 유닛](../deploy/systemd/fillwire-serve.service.example),
  [serve 설정](../deploy/systemd/serve.toml.example),
  [quote-collector 유닛](../deploy/systemd/quote-collector.service.example),
  [배포 스크립트](../scripts/deploy-serve.sh)를 각 유닛의 경로에 맞춰 복사합니다.
  유닛 사본의 `.example`을 빼고 스크립트는 `/usr/local/sbin/deploy-serve.sh`에 둡니다.
  유닛과 핀 디렉터리는 root 소유로 유지합니다. 읽기 전용 설정과 달력·대상 파일은
  컨테이너 UID 65532가 읽을 수 있게, 각 `/data`의 호스트 디렉터리는 쓸 수 있게 준비합니다.
- gatewayd는 먼저 `POST /v1/tokens/{provider}/fetch`를 지원하는 기존 인스턴스를
  사용합니다. `GATEWAYD_FETCH_CONSUMERS`에서 소비자별 권한을 확인합니다. serve에는
  `kis-live-approval`, collector에는 `kis-live` 권한을 부여하고 실제 인증값은 각 유닛이
  참조하는 호스트 설정으로 전달합니다. Toss를 켜면 같은 클라이언트의 발급자를
  gatewayd 한 곳으로 통일하고 `toss` 조회 권한을 추가합니다.
- CI의 `Build and push immutable image` 요약에서 `ghcr.io/mgh3326/fillwire@sha256:…`
  전체 값을 확인합니다. 아래 `PIN`에 이 값을 지정합니다. 두 유닛은 각각 256 MiB 상한을
  사용합니다. pi의 실제 사용량과 OOM 여부를 그림자 기간에 확인합니다.
- 스크립트는 arm64 다이제스트를 당겨 확인한 뒤 현재 핀을 교체합니다. serve는 컨테이너의
  실제 이미지와 `/healthz` 200, collector는 컨테이너의 실제 이미지와 실행 상태를 검사합니다.
  두 검사와 별도로 기록의 신선도를 확인합니다. 성공한 교체의 직전 핀은 `.previous`에 남습니다.
  이전 이미지의 로컬 캐시는 롤백을 위해 보존합니다. 배포가 강제 종료되어
  `<unit>.service.lock`이 남으면 기존 배포 프로세스가 종료됐는지 확인하고, 현재 컨테이너
  이미지와 핀을 대조한 뒤 잠금을 제거합니다.

```sh
sudo systemctl daemon-reload
sudo bash /usr/local/sbin/deploy-serve.sh fillwire-serve "$PIN"
sudo bash /usr/local/sbin/deploy-serve.sh quote-collector "$PIN"
curl --fail http://127.0.0.1:8792/healthz
```

동일 호스트에서 이미지를 직접 실행할 때의 CLI 형식입니다. 첫 줄은 serve 유닛의
인수입니다. 두 번째는 격리된 Redis에 collector의 출력도 남기는 운용 예입니다.
Redis를 쓰면 키는 `minute:kis:<venue>:<symbol>`, 상한은 키별 `MAXLEN ~ 100000`입니다.
유닛 사본은 그림자 비교용 JSONL만 사용합니다.

```sh
fillwire serve -config /etc/fillwire/serve.toml
fillwire market-tap -enabled -targets /etc/fillwire/targets.json -calendar /etc/fillwire/calendar.json -out /data/bars.jsonl -token-source gatewayd-fetch -gatewayd-url http://127.0.0.1:8791 -gatewayd-token-env GATEWAYD_FETCH_TOKEN -gatewayd-provider kis-live -redis redis://127.0.0.1:6380/0
```

## 그림자와 병행

1. **그림자**. 기존 유닛과 Python Dagu 잡을 유지합니다. serve 사본은 모든 모듈이
   꺼진 상태에서 시작합니다. 별도 디렉터리의 공개 Upbit 기록부터 켜고, 잔여 연결
   한도가 확인된 Toss·Upbit 비공개 피드는 JSONL에만 기록합니다. KIS는 app key당
   웹소켓 한 개만 허용합니다. 병행이 승인된 별도 키가 있을 때만 KIS WS 그림자를
   시작합니다. 동일 키라면 합의된 인계 창에서 기존 WS를 먼저 내린 뒤 확인합니다.
   그림자 KIS는 `[kis_fills]`를 끄고 선물은 `jsonl_path`로 기록합니다. 승격 호가처럼
   Redis 키가 코드에 고정된 모듈은 별도의 Redis를 사용합니다. `[kis_quotes].max_len`,
   `[kis_futures].max_len`, `[stream].max_len`은 스트림당 100000을 적용합니다.
2. **분봉 비교**. collector는 gatewayd 조회 토큰을 메모리에 보관하며 KIS REST 분봉을
   JSONL에 기록합니다. [market-tap](market-tap.md)의 KR/US 대상과 달력 파일을 같은
   거래일 기준으로 맞춥니다. Python이 계속 research 표를 씁니다. broker별 HTTP
   예산에는 양쪽 호출을 합산합니다. 운영 측 KIS 429가 증가하면 그림자 폴러를 내립니다.
3. **병행 쓰기**. Go ingest가 마련된 뒤에만 진행합니다. 삽입 전용 research 표에서
   먼저 도착한 행이 남으므로 Python 행을 비교할 수 있게 그림자 관측치를 보존합니다.
   auto_trader의 `scripts.compare_go_py_minutes`로 같은 거래일의 Go JSONL과 Python 행을
   `(time_utc, symbol, venue)`로 조인합니다. OHLCV, value, session_segment,
   양방향 누락을 비교하고 `diagnose_session_coverage.py`의 최초 수신 p50/p95와 양쪽
   429·회로 열림 횟수, 서빙 KIS 429를 따로 기록합니다. KIS WS는 닫힌 선물 봉과
   volume_gap, 체결 건수·최신 시각, Toss/Upbit는 기록 지연과 재접속 누락도 확인합니다.
   기록 보관 기간과 디스크 증가량은 그림자 기간 동안 확인합니다.

## 6321 §6 전환 기준

아래 다섯 항목을 모두 충족해야 합니다.

1. 연속 5 거래일 닫힌 봉 OHLCV 일치 100% (형성 중 봉 제외).
2. 고정 계층 결측 0.
3. Go의 429·회로 open이 Python보다 많지 않음.
4. 최초 수신 p95가 Python 이하.
5. 서빙 프로세스의 KIS 429 증가 없음.

비교 도구의 PASS는 종목·세션 단위 커버리지 판정입니다. Go JSONL에 최초 수신 시각이
없어서 기준 4는 별도 측정이 준비될 때까지 미확인입니다. 현재 market-tap은 JSONL/Redis에
기록하고 research ingest는 수행하지 않습니다. 이 사본으로는 단계 0을 수행할 수 있습니다.

## 기존 유닛 인계 순서

그림자 비교와 운영자 승인 후 다음 순서로 소유권을 넘깁니다. 각 단계에서 기존 컨테이너와
리스너가 내려간 것을 확인하고 새 출력이 도착한 뒤 다음으로 넘어갑니다.

1. `fillwire-rec`를 정지하고 serve의 Upbit 공개 기록을 운영 디렉터리로 전환합니다.
2. `fillwire.service`를 정지합니다. 같은 키의 소유자가 `at-kis-ws`인 배치에서는 그
   컨테이너도 내려가 있는지 확인합니다. 운영 Redis 키와 ingest 설정을 준비한 serve가 KIS 체결·시세·선물 연결을 소유합니다.
3. `broker-edge-canary.timer` 등 기존 주문 호출자를 먼저 중지할 수 있는지 확인합니다.
   모의 주문 기능을 계속 쓰면 `kis-mock-edge`와 이를 실행하는 `broker-edge`를 유지합니다.
   주문 호출자가 없을 때만 해당 컨테이너의 실제 소유 유닛을 정지합니다.
   `broker-edge.service`가 `kis-mock-edge`를 실행하는 배치에서는 한 번만 정지합니다.
   serve의 WS 기록기를 주문 기능의 대체로 취급하지 않습니다.
4. gatewayd를 serve로 옮기는 경우, 기존 gatewayd의 공급자·조회 권한과 토큰 갱신
   정책을 옮긴 뒤 기존 발급자를 정지하고 serve의 `[gatewayd]`를 기동합니다. gatewayd가
   broker-edge에 함께 묶인 배치에서는 3단계 정지 직후 이전합니다. 전환 전후에 토큰
   조회와 소비자 연결을 확인합니다. 기존 이미지와 유닛은 롤백에 쓸 수 있게 보존합니다.

## Dagu 잡의 대체와 유지

| 잡 | 전환 결론 | 근거와 종료 조건 |
|---|---|---|
| `at-session-minutes-kr` | 대체 후보. 현재 유지 | collector가 KRX·NXT REST 분봉을 기록합니다. research ingest, 종목 갱신·갭 처리와 기준 ①–⑤ 확인 뒤 운영자가 중지합니다. |
| `at-session-minutes-us` | 유지 | collector의 KIS 미국 분봉은 비교할 수 있습니다. Toss 주간장 커버리지와 research 쓰기가 필요합니다. |
| `at-intraday-movers` | 유지 | 전체 국내 등락·거래대금 순위 관측을 수행합니다. 고정·승격 종목의 시세 스트림만으로 순위를 재현할 수 없습니다. |
| `at-us-intraday-movers` | 유지 | 미국 등락·거래량 순위를 수집합니다. IEX·Toss 시세 기록은 순위 데이터와 커버리지가 다릅니다. |
| `at-futures-minutes-day` | 대체 후보. 현재 유지 | `kis_futures`가 닫힌 선물 봉을 내보냅니다. research 선물 ingest(hk 6430)와 거래일·계약·OHLCV·갭 검증이 끝나야 중지할 수 있습니다. |
| `at-kr-news-timing` | 유지 | 네이버 뉴스 수집입니다. |
| `at-saveticker-shadow` | 유지 | Save+ 뉴스 JSONL 수집입니다. |
| `at-toss-fill-poll` | 유지 | 체결 발견과 원장 대사를 수행합니다. `toss_ws`는 그림자 기록만 합니다. 원장 대사와 재접속 누락 복구가 이관될 때 종료를 판단합니다. |
| `at-upbit-1m` | 유지 | 닫힌 KRW 분봉의 집계와 DB 적재를 수행합니다. `upbit`는 원시 공개 체결·호가를 기록합니다. |

설계 기준은 auto_trader
[6321 §6–§7](https://github.com/mgh3326/auto_trader/blob/a1fa87b934519200b3fda289bf631f8ea50f952d/docs/design/6321-go-quote-collector.md)입니다.

## 롤백

이미지 변경만 되돌릴 때는 이전 다이제스트로 전환하고 상태와 새로 도착한 행을 확인합니다.

```sh
sudo bash /usr/local/sbin/deploy-serve.sh fillwire-serve --rollback
sudo bash /usr/local/sbin/deploy-serve.sh quote-collector --rollback
```

데이터 경로를 되돌릴 때는 collector를 먼저 내리고 Python 수집이 다시 쓰는지 확인합니다.
serve 롤백은 새 serve를 멈춰 토큰 발급과 KIS 세션이 종료된 뒤 기존 gatewayd,
broker-edge(`kis-mock-edge`), `fillwire.service`, `fillwire-rec` 순으로 실제로 내렸던
유닛을 복구합니다. 정지한 Dagu 잡은 해당 운영 테이블과 누락 범위를 확인한 뒤 재개합니다.
Toss 원장 대사는 단일 실행자를 유지합니다. 운영 설정과 Redis 소비자 라우팅도 이전
사본으로 복원합니다.

`kis-mock-edge`와 `edge-canary`는 기존 모의 주문 경로와 인계 기간에 필요하므로
보존합니다. KIS 이력 후보가 한 건이어도 주문 소유권을 확정할 수 없으므로 UNKNOWN을
유지합니다. canary의 HTTP 단계 제한은 응답 본문 수신까지 적용됩니다.
