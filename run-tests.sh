#!/bin/sh
sudo docker run --name postgres -e POSTGRES_PASSWORD=postgres -d -p 5432:5432 postgres 
sleep 10
sudo docker cp PostgresSupplyCollectorTests/tests/data.sql postgres:/docker-entrypoint-initdb.d/data.sql
sudo docker exec -u postgres postgres psql postgres postgres -f docker-entrypoint-initdb.d/data.sql
dotnet test
sudo docker stop postgres
sudo docker rm postgres
