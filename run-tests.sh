#!/bin/sh
docker run --name postgres -e POSTGRES_PASSWORD=postgres -d -p 5432:5432 postgres 
export POSTGRES_DB=postgres
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=postgres
export POSTGRES_HOST=localhost
sleep 10
docker cp PostgresSupplyCollectorLoader/tests/data.sql postgres:/docker-entrypoint-initdb.d/data.sql
docker exec -u postgres postgres psql postgres postgres -f docker-entrypoint-initdb.d/data.sql
dotnet test
docker stop postgres
docker rm postgres
