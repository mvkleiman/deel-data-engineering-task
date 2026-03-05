echo "Connectors"
curl -s "http://localhost:8083/connectors" | jq .

echo "postgres-source status"
curl -s "http://localhost:8083/connectors/postgres-source/status" | jq .