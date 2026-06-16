# 03-scenario-a.ps1
#
# Scenario A: Massive Sensor Ingestion - Sekcija 3
#
# Postavlja zadati broj simuliranih uredjaja (DEVICE_COUNT) u Kafka modu
# (acks=1, fiksirano za fer poredjenje) i mери throughput / gubitak poruka
# u sensor_data tokom ~60 sekundi.
#
# Upotreba:
#   .\03-scenario-a.ps1 -DeviceCount 100
#   .\03-scenario-a.ps1 -DeviceCount 1000
#   .\03-scenario-a.ps1 -DeviceCount 10000   # OPREZ: visoko opterecenje,
#                                             # vidi napomenu u izvestaju
#
# Pokrece se iz root foldera projekta. Preduslov: Docker Desktop pokrenut,
# ceo stack vec gore.

param(
    [Parameter(Mandatory=$true)]
    [int]$DeviceCount,

    [int]$IntervalMs = 2000,

    [int]$DurationSeconds = 60
)

$ErrorActionPreference = "Stop"

Write-Host "=== Scenario A: DEVICE_COUNT=$DeviceCount, INTERVAL_MS=$IntervalMs ===" -ForegroundColor Cyan

if ($DeviceCount -ge 10000) {
    Write-Host "UPOZORENJE: DEVICE_COUNT >= 10000 moze znacajno opteretiti CPU/RAM" -ForegroundColor Red
    Write-Host "Pratite 'docker stats' tokom testa i prekinite (Ctrl+C, pa docker compose stop data-ingestion)" -ForegroundColor Red
    Write-Host "ako CPU/MEM nastavi da raste bez stabilizacije." -ForegroundColor Red
}

# 1. Override env varijable
$override = @"
services:
  data-ingestion:
    environment:
      BROKER_TYPE: kafka
      KAFKA_ACKS: "1"
      DEVICE_COUNT: "$DeviceCount"
      INTERVAL_MS: "$IntervalMs"
  analytics:
    environment:
      BROKER_TYPE: kafka
  data-storage:
    environment:
      Broker__Mode: kafka
"@
$override | Out-File -Encoding utf8 docker-compose.override.yml

# 2. Rebuild
docker compose up -d --build data-ingestion analytics data-storage
Start-Sleep -Seconds 10

docker logs iot-ingestion --tail 6

# 3. Helper
function Get-RowCount {
    (docker exec iot-postgres psql -U postgres -d iot_db -t -A -c "SELECT COUNT(*) FROM sensor_data;").Trim()
}

# 4. A - pocetak merenja
$A = Get-RowCount
$startTime = Get-Date
Write-Host "A (t=0) = $A"

# 5. Cekanje DurationSeconds
Write-Host "Cekanje $DurationSeconds sekundi..."
Start-Sleep -Seconds $DurationSeconds

# 6. B - kraj merenja
$B = Get-RowCount
$endTime = Get-Date
Write-Host "B (t=${DurationSeconds}s) = $B"

$elapsed = ($endTime - $startTime).TotalSeconds
$delta = [int]$B - [int]$A
$expected = [math]::Round($DeviceCount * ($elapsed * 1000 / $IntervalMs))
$throughput = [math]::Round($delta / $elapsed, 2)
$lossPct = if ($expected -gt 0) { [math]::Round((($expected - $delta) / $expected) * 100, 2) } else { 0 }

Write-Host ""
Write-Host "================ REZULTAT ================" -ForegroundColor Green
Write-Host "DEVICE_COUNT     = $DeviceCount"
Write-Host "Stvarno trajanje = $([math]::Round($elapsed,1)) s"
Write-Host "A                = $A"
Write-Host "B                = $B"
Write-Host "Delta            = $delta"
Write-Host "Ocekivano        = $expected"
Write-Host "Throughput       = $throughput msg/s"
Write-Host "Procena gubitka  = $lossPct %  (negativno = vise od ocekivanog, normalno zbog manualnog tajminga)"
Write-Host ""
Write-Host "docker stats snapshot:" -ForegroundColor Yellow
docker stats --no-stream
