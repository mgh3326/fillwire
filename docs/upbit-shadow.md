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

## 체결 재생 (`execsim -venue upbit`)

기록기(`fillwire upbit`)가 쓴 `orderbook`/`trade` 프레임을 `Plan`으로 재생하고,
그 자식 주문이 녹화된 호가에서 어떻게 체결됐을지 계산합니다. 규칙의 권위는
auto_trader `docs/design/5852-upbit-krw-shadow-strategy.md` §7.2 규칙 1~7입니다.
주문을 보내지 않고 네트워크도 쓰지 않습니다(`cmd/execsim/noorder_test.go`가
바이너리에서 확인). 같은 녹화와 같은 의도는 바이트 단위로 같은 출력을 냅니다.

```sh
go run ./cmd/execsim -venue upbit -intents INTENTS.jsonl \
  -books var/upbit/2026-11-19/KRW-XRP.jsonl [-books ...] [-stale 2s]
```

`-books`는 시간순 기록 파일이며 여러 번 쓸 수 있습니다(닫힌 날은 `.jsonl.gz`).
KIS 모드(`-venue kis`, 기본값)는 바뀌지 않았습니다.

### 의도

한 줄에 의도 하나입니다. 위 의도 필드에 더해 재생 전용 선택 필드 둘이 있습니다.
`cmd/execupbit`는 이 필드를 알 수 없는 필드로 거부합니다.

- `issued_at`: 의도 발행(슬롯) 시각. 없으면 첫 기록에서 시작합니다.
- `stop_price_krw`: 손절 매도. 매도에만 쓰고 `issued_at`이 필요합니다.

### 출력

의도마다 `through`, `touch` 순서로(손절 의도는 `slot`, `continuous` 트리거를
각각) 자식 줄(`kind:"child"`)들과 결과 줄(`kind:"result"`)을 냅니다. 규칙마다
플랜이 자기 규칙의 체결만 보고 결정하므로 두 규칙은 따로 셉니다. 수량은 소수점
문자열, 가격은 정수 원이고 `fill_notional_krw`는 정확한 합, `avg_price_krw`는
그 값을 소수 둘째 자리로 반올림(half up)한 값입니다. float는 쓰지 않습니다.

자식 `status`: `active`(도착, `fills`에 체결), `void_gap`(gap이 열릴 때 비행 중),
`cancelled`(도착 전에 취소), `no_working_order`(정정할 주문이 없음),
`not_arrived`(녹화가 끝남), `applied`(취소). 결과 `done`은 플랜의 `filled`,
`deadline`에 더해 `not_triggered`(손절이 발동하지 않음), `unfinished`(녹화가
만료 전에 끝남)입니다.

### 규칙을 이렇게 읽었습니다

설계 문장이 열어 둔 곳의 해석입니다. 바꾸려면 설계 문서가 먼저 바뀌어야 합니다.

1. 자식은 발행 시각 `τ`(가격을 정한 호가의 수신 시각) + 1.0초 **이후 첫 호가
   프레임**(같은 시각 포함)에 도착합니다. 1초는 고정값이며 플래그가 아닙니다.
   플랜은 그 기록 자신의 체결을 보지 못하고 다음 기록부터 봅니다.
2. 도착 프레임의 반대편 호가를 좋은 쪽부터 지정가 이내로 단계별 표시 수량만큼
   그 단계 가격에 체결하고, 남은 수량은 지정가에 대기합니다(15단계 밖 포함).
3. 대기 체결 가격은 항상 주문의 지정가입니다. `through`는 지정가를 **엄격히**
   통과한 체결(`trade`)의 수량만큼, `touch`는 지정가에 닿은 체결 또는 반대편
   호가에 닿으면 남은 전부를 채웁니다. 큐 순서는 없습니다. 도착 전 체결은 닿지
   않습니다.
4. `expires_at` 이후(그 시각 포함) 기록은 채우지 않습니다.
5. `gap` 기록이 창을 열고 다음 `connected` 기록(또는 다음 프레임)이 닫습니다.
   열릴 때 비행 중인 자식은 무효(`void_gap`)이고 창 안의 체결은 인정하지 않습니다.
   60초를 넘는 창이 **포지션을 든 채** 열리면 `gap_exposed`입니다. 매수는 첫
   체결 이후 녹화 끝까지, 매도는 실행 시작부터 완료까지 포지션이 있다고 봅니다.
   무효가 된 제출은 플랜 안에서 계속 "작업 중"으로 남고 다시 내지 않습니다.
6. 보고는 항상 `through`와 `touch` 둘 다입니다. `BRACKET_SENSITIVE` 판정은
   T7 하네스의 일입니다.
7. 손절 `slot`(판정에 쓰는 값): `issued_at` 이전 마지막 호가(나이 ≤ `-stale`)의
   최우선 매수호가가 `stop_price_krw` 이하일 때만 시작하며, 아니면 `not_triggered`로
   끝납니다. 슬롯 시각 `issued_at`에 그 호가로 가격을 정하고 +1초 이후 프레임에서
   체결합니다. `continuous`(보조): `issued_at` 이후 처음으로 최우선 매수호가가
   손절가 이하인 프레임에서 시작합니다.

### 한계

- 지정가는 정수 원 틱 격자(100 KRW 이상)만 다룹니다. 마켓은 `MarketNames` 네 개로
  제한됩니다.
- 깊이 프레임은 15단계 전체를 읽고, 한 단계라도 비었거나 순서·격자가 어긋나면
  프레임 전체를 거부(`refused_frames`)합니다. 거부된 프레임으로는 아무것도
  가격을 정하지 않습니다.
- 체결 프레임 형식(`trade_price`, `trade_volume`)은 Upbit 공개 웹소켓 문서
  기준이며 녹화된 실제 `trade` 프레임으로 확인하지는 못했습니다.
