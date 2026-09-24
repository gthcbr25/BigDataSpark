#!/usr/bin/env bash
# Скачивает JDBC-драйверы PostgreSQL и ClickHouse в каталог jars/.
# Драйверы в репозиторий не коммитятся (см. .gitignore).
set -euo pipefail

mkdir -p jars
curl -fL "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar" \
    -o "jars/postgresql-42.7.4.jar"
curl -fL "https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.0/clickhouse-jdbc-0.6.0-all.jar" \
    -o "jars/clickhouse-jdbc-0.6.0-all.jar"
ls -lh jars/*.jar
