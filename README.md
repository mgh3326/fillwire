# fillwire

Fillwire listens to broker execution feeds, persists decoded fills to Redis Streams, and delivers them through independent consumer groups.

It is a runtime, not a broker client: protocol code lives in separate libraries (e.g. `go-kis`). Fillwire never places orders — it listens, persists, and delivers.

Status: design (see `docs/design.md`, coming with the first PR).
