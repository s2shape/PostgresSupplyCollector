#!/bin/sh
docker run --name postgres -e POSTGRES_PASSWORD=postgres -d -p 5432:5432 postgres 
sleep 10
docker cp PostgresSupplyCollectorTests/tests/data.sql postgres:/docker-entrypoint-initdb.d/data.sql
docker exec -u postgres postgres psql postgres postgres -f docker-entrypoint-initdb.d/data.sql
dotnet test
docker stop postgres
docker rm postgres
