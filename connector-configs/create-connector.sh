curl -s -X POST "http://localhost:8083/connectors" \
    -H "Content-Type: application/json" \
    -d @postgres-source.json | jq