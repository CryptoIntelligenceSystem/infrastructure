# Apply every clickhouse/init/*.sql (in name order) to the running ClickHouse container.
# Safe on an existing database: all statements are CREATE/ALTER ... IF NOT EXISTS. Never removes volumes.
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

$client = 'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD"'
foreach ($file in Get-ChildItem clickhouse/init/*.sql | Sort-Object Name) {
    Write-Host "Applying $($file.Name)"
    Get-Content -Raw $file.FullName | docker compose exec -T clickhouse sh -c "$client --multiquery"
    if ($LASTEXITCODE -ne 0) { throw "Migration failed: $($file.Name)" }
}
docker compose exec -T clickhouse sh -c "$client --query 'SHOW TABLES FROM pipeline'"
