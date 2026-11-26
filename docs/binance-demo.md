# Binance Spot Demo 읽기 전용 스트림

`fillwire binance-demo`는 Spot Demo의 시장 스트림과 선택한 사용자 스트림을 수집해 `internal/recording` 일별 JSONL 파일에만 기록합니다. 체결 알림은 관찰 자료이며 원장이나 주문 처리로 전달하지 않습니다.

기본값은 비활성입니다. 명시적으로 켜고 포그라운드에서 실행합니다.

```bash
BINANCE_SPOT_DEMO_STREAMS_ENABLED=true \
  fillwire binance-demo --symbols BTCUSDT,ETHUSDT --record-dir <기록-디렉터리>
```

사용자 스트림을 끄면 자격 증명 없이 시세만 기록합니다.

```bash
BINANCE_SPOT_DEMO_STREAMS_ENABLED=true \
  fillwire binance-demo --symbols BTCUSDT --record-dir <기록-디렉터리> --no-user-stream
```

사용자 스트림 자격 증명은 `BINANCE_SPOT_DEMO_API_KEY`와 `BINANCE_SPOT_DEMO_API_SECRET`에서 읽습니다. 제품별 두 변수가 모두 없을 때만 `BINANCE_DEMO_API_KEY`와 `BINANCE_DEMO_API_SECRET` 쌍을 사용합니다. 일부만 설정된 쌍은 다른 쌍과 섞지 않고 거부합니다. 값은 로그와 기록에 포함하지 않습니다.

시장 URL은 `demo-stream.binance.com`의 `/stream` 또는 `/ws`, 사용자 URL은 `demo-ws-api.binance.com/ws-api/v3`로 고정됩니다. 코드는 다른 호스트, 포트, 경로를 연결 전에 거부하고 프록시와 리다이렉트를 사용하지 않습니다. 재연결은 최대 30초 지수 백오프를 적용합니다.

기록은 `<기록-디렉터리>/binance-demo/<UTC 날짜>/<심볼 또는 user>.jsonl`에 추가됩니다. 각 레코드의 `received_at`은 로컬 수신 시각입니다. 잘못된 프레임은 버리고 건강 상태의 계수에 반영합니다. 사용자 구독 거절은 상태와 오류 코드만 기록하며 시장 스트림은 계속 실행합니다.
