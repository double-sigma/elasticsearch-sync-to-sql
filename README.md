docker build --platform linux/amd64 --tag "elasticsearch_sync:local.1" .

docker run -p 8080:8080 elasticsearch_sync:local.1

Start sync:
curl -X POST 'http://localhost:8080/api/sync?startDate=2024-01-01T00:00:00Z'

See the log:
http://localhost:8080/api/log

See the data:
http://localhost:8080/api/data?page=1