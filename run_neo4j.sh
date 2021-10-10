#!/bin/sh

docker run \
    --name jp-neo4j \
    -p7474:7474 -p7687:7687 \
    -d \
    -v $HOME/neo4j/data:/data \
    -v $HOME/neo4j/logs:/logs \
    -v $HOME/neo4j/import:/var/lib/neo4j/import \
    -v $HOME/neo4j/plugins:/plugins \
    --env NEO4J_AUTH=neo4j/japanese \
    --env NEO4JLABS_PLUGINS='["apoc", "graph-data-science"]' \
    --env NEO4J_apoc_import_file_enabled=true \
    --env NEO4J_apoc_export_file_enabled=true \
    neo4j:latest