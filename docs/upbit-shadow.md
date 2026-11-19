# Upbit 그림자 실행기 (`execupbit`) — 주문 없음

이 문서는 Upbit 공개 호가로만 움직이는 그림자 실행기(`cmd/execupbit`,
`internal/exec/upbit`)의 실행 방법과 하지 않는 일을 정리합니다. Upbit 자식 주문
전송은 아직 배선되지 않았습니다. 전송 계약은 auto_trader hk 5438에서 정합니다.

## 하지 않는 일

- **주문을 보내지 않습니다.** 전송 플래그가 없고, E1 게이트웨이 클라이언트
  (`internal/exec/e1`)와 KIS 실행기(`internal/exec`)를 링크하지 않습니다
  (`cmd/execupbit/noorder_test.go`가 검사). `-send` 같은 플래그는 알 수 없는
  플래그로 거부됩니다.
- 인증이 필요 없습니다. Upbit 공개 websocket만 읽고, API 키·토큰을 읽지 않습니다.
- E1 자식 형식(`internal/exec/e1` wire format)과 KIS 모의 경로를 바꾸지 않습니다.
  E1의 계좌 허용 목록은 그대로 `kis_mock` 하나입니다.
- 지갑·잔고·체결을 읽지 않습니다. 체결원이 없으므로 잔여 수량은 의도(intent)의
  목표 수량에서 채워진 양을 뺀 값으로만 계산합니다(`Plan.Fill`).

## 실행

녹음된 호가 시퀀스 재생(결정적, 골든 고정):

```sh
go run ./cmd/execupbit -dry-run \
  -intent cmd/execupbit/testdata/intent.json \
  -books cmd/execupbit/testdata/books.jsonl
```

실시간 공개 websocket(그림자). `-root`는 `internal/upbit` 녹음기가 증거를 쓰는
디렉터리이며 필수입니다:

```sh
go run ./cmd/execupbit -dry-run -intent INTENT.json -live -root VAR/upbit-shadow -duration 1m
```

`-stale`(기본 2s)은 자식 주문 가격을 계산할 수 있는 호가의 최대 나이입니다.

## 출력

한 줄에 JSON 하나입니다.

- `{"kind":"book",...}`: 호가 이벤트마다 한 줄. `decision`은 `hold`, `submit`,
  `amend`, `cancel`, `finish` 중 하나이고 `reason`이 이유입니다.
- `{"kind":"child",...}`: 보냈을 자식 요청. `quantity`는 소수점 문자열(최대 8자리),
  `limit_price_krw`는 정수 원입니다. 가격·수량에 float를 쓰지 않습니다.
- `{"kind":"gap",...}`: websocket 단절. 재생 모드에서는 이 시점에 만료 검사를
  한 번 수행합니다.
- `{"kind":"finish",...}`: 의도 종료(`done`은 `filled` 또는 `deadline`).

## 정책

- **테이커 가격**: 매수는 최우선 매도호가, 매도는 최우선 매수호가를 체결 방향으로
  호가 단위에 맞춘 값. 의도의 가격 밴드(`limit_price_krw` ± `max_slippage_bps`)를
  벗어나면 아무것도 하지 않습니다(`reason:"band"`).
- **추격(re-quote)**: 최우선 호가가 우리 주문보다 불리한 쪽으로 움직일 때만
  `amend`합니다. `max_requotes`회를 넘으면 `reason:"requote_limit"`으로 대기합니다.
- **만료**: `expires_at`에 도달하면 남은 주문을 `cancel`하고 `deadline`으로
  종료합니다. 호가가 끊겨도 1초 tick이 만료를 지킵니다.
- **한도**: 자식 수량은 잔여 수량과 잔여 명목(`max_notional_krw` - 기체결 명목)을
  넘지 않습니다. Upbit KRW 마켓 최소 주문 금액 5,000 KRW 미만이면 보내지 않습니다.
- **오래된 호가**: `-stale`보다 오래된 호가, 미래 시각 호가, 다른 마켓의 호가는
  새 주문·정정 가격으로 쓰지 않습니다.

## 호가 단위

Upbit KRW 마켓의 주문 가격 단위는 2025-07-31 이후 표를 씁니다
([원화(KRW) 마켓 주문 가격 단위](https://docs.upbit.com/kr/docs/krw-market-info),
[변경 공지](https://docs.upbit.com/kr/changelog/krw_tick_unit_change_250731)).
`internal/exec/upbit/tick.go`의 표와 `tick_test.go`가 이 표를 그대로 검사합니다.
이 모델은 정수 원 가격만 다루므로 100 KRW 미만(소수 호가 단위) 구간은 거부합니다.

## 의도(intent) 예시

```json
{"schema_version":1,"intent_id":"2d3c4b5a-6e7f-4a1b-8c2d-3e4f5a6b7c8d",
 "venue":"upbit","account":"upbit_shadow","market":"KRW-XRP","side":"buy",
 "max_quantity":"33","max_notional_krw":100000,"limit_price_krw":3000,
 "max_slippage_bps":100,"max_requotes":2,"expires_at":"2026-11-19T09:00:02.5Z"}
```

`venue`, `account`, 자식 필드 이름은 이 패키지 안에서만 쓰는 로컬 이름입니다.
hk 5438이 Upbit wire 계약을 확정하면 그때 로컬에서 바꿉니다.
