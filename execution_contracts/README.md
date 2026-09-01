# execution_contracts

`ExecutionCommandV1`, `ExecutionReceiptV1`, and `TokenLeaseView` are small named
JSON shapes for future HTTPS integration. They are not a ledger, broker gateway,
or token transport.

Python은 지금 이 스키마로 호출하지 않는다. 현재 구현은 `kis-mock-read` CLI뿐이며,
향후 Python 연결은 별도 승인된 HTTPS 경계에서만 검토한다.
