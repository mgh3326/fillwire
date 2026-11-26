# Toss 주문 웹소켓 그림자 기록

`fillwire` 실행 시 기본값은 비활성화입니다. 활성화하려면 다음 표를 추가합니다. `account_seq`에는 Toss 계좌 목록의 숫자 `accountSeq`를 문자열로 넣습니다. 기존 `[kis.gatewayd]`의 URL과 `Authorization`용 `token_env`를 사용하며, gatewayd 소비자에 `toss` 제공자 접근 권한이 필요합니다.

```toml
[toss_ws]
enabled = true
account_seq = "3"
directory = "var/toss-ws"
```

리더는 `personal:order` 하나를 선언하며 연결을 한 개만 유지합니다. 계좌당 두 개를 초과하면 Toss가 가장 오래된 연결을 종료합니다. 예기치 않은 종료에서 `possible connection eviction` 로그를 확인하고, 동일 계좌의 다른 웹소켓 클라이언트 수를 확인합니다. `account-not-found`로 리더가 중지되면 설정한 계좌가 본인 소유의 활성 종합매매 계좌인지 확인합니다.

기록은 `<directory>/toss-order/<UTC 날짜>/<account_seq>.jsonl`에 추가됩니다. 실행 중 날짜가 바뀌어 닫힌 파일은 gzip으로 압축됩니다. 종료 시 닫힌 파일은 압축하지 않습니다. 세션마다 접속 직후 `resync_needed` 한 줄을 쓰고, 구독 확인 후 받은 유효한 주문 이벤트를 `order` 줄에 추가합니다. 각 줄에는 UTC `received_at`, `session_id`, `topic`이 들어가며 주문 줄에는 검증한 `data`와 원본 프레임 `raw`도 들어갑니다. 계좌 주문 내역을 담으므로 기존 개인 주문 기록과 같은 접근 정책을 적용합니다.

## REST 폴러와 비교

1. 같은 계좌와 UTC 시간 범위의 JSONL과 `at-toss-fill-poll` 실행 기록을 확보합니다. `.jsonl`은 `cat`으로, `.jsonl.gz`는 `gzip -dc`로 읽습니다.
2. `data.event`가 `PARTIAL_FILL` 또는 `FILL`인 줄을 고릅니다. `data.order.orderId`와 누적 `data.order.execution.filledQuantity`로 폴러가 처음 확인한 주문 상태와 대응시킵니다. 폴러 관측 시각에서 `received_at`을 빼 두 경로의 관측 시간 차이를 계산합니다. 폴러 한 번에 여러 부분 체결이 합쳐질 수 있으므로 수량이 정확히 대응하지 않는 경우는 따로 집계합니다.
3. `session_id`가 바뀌는 지점에서 폴러에만 나타난 상태와 웹소켓에만 나타난 상태를 확인합니다. `resync_needed`는 새 세션의 시작 시각이며 직전 연결이 끊긴 정확한 시각은 종료 로그에서 확인합니다. 연결이 끊긴 동안의 이벤트는 재전송되지 않습니다. 이 리더의 동작 범위는 JSONL 기록까지입니다.

[Toss Open API 개요](https://openapi.tossinvest.com/openapi-docs/overview.md#웹소켓-연동) · [AsyncAPI Connection / Order Event](https://openapi.tossinvest.com/openapi-docs/latest/asyncapi.json)
