# Postgres KV store benchamrks

## Setting up DB
```sh
docker-compose up -d
# if tern is available
cd migrations
tern migrate
# else
psql -h 127.0.0.1 -U postgres -d kvstore < migrations/*.sql
```

## Benchmarking
```sh
go test ./bench -bench=.
```
