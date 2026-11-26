# KIS mock websocket fixtures

The field order fixtures follow the official KIS websocket sample definitions:

- Overseas mock fill notice `H0GSCNI9` and its 25 columns: [overseas_stock_functions_ws.py](https://github.com/koreainvestment/open-trading-api/blob/main/examples_user/overseas_stock/overseas_stock_functions_ws.py#L75-L142).
- Domestic futures/options fill notice `H0IFCNI0`, supported by live and mock, and its 22 columns: [domestic_futureoption_functions_ws.py](https://github.com/koreainvestment/open-trading-api/blob/main/examples_user/domestic_futureoption/domestic_futureoption_functions_ws.py#L195-L250).
- Domestic quote frames `H0STCNT0` and `H0STASP0` use the official example frame layout and the shared `internal/book` decoder; both websocket TR IDs are the same for mock: [asking_price_krx.py](https://github.com/koreainvestment/open-trading-api/blob/main/examples_llm/domestic_stock/asking_price_krx/asking_price_krx.py).

Tests generate deterministic synthetic values in those official column orders. No live account payloads or credentials are included.
