curl -s -X PUT "http://localhost:8083/connectors/postgres-source/config" \
    -H "Content-Type: application/json" \
    -d @postgres-source.json | jq