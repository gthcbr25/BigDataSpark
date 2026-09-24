# Скачивает JDBC-драйверы PostgreSQL и ClickHouse в каталог jars/ (вариант для Windows).
$ErrorActionPreference = 'Stop'

New-Item -ItemType Directory -Force jars | Out-Null
Invoke-WebRequest -Uri 'https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar' -OutFile 'jars\postgresql-42.7.4.jar'
Invoke-WebRequest -Uri 'https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.0/clickhouse-jdbc-0.6.0-all.jar' -OutFile 'jars\clickhouse-jdbc-0.6.0-all.jar'
Get-ChildItem .\jars\*.jar | Select-Object Name, Length
