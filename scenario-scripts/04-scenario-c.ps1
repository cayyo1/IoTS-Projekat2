# 04-scenario-c.ps1
#
# Scenario C: Burst Event Load - Sekcija 5
#
# Tri faze:
#   1. Baseline - DEVICE_COUNT=BaselineDevices, 30s
#   2. Burst    - DEVICE_COUNT=BurstDevices,    30s (naglo skaliranje)
#   3. Recovery - DEVICE_COUNT=BaselineDevices, prati Consumer Lag dok ne
#                  padne i stabilizuje se (4x provera na 20s)
#
# Upotreba (default 100 -> 2000 -> 100):
#   .\04-scenario-c.ps1
#   .\04-scenario-c.ps1 -BaselineDevices 100 -BurstDevices 2000
#
# Pokrece se iz root foldera projekta. Preduslov: Docker Desktop pokrenut,
# ceo stack vec gore, Kafka mod.

param(
    [int]$BaselineDevices = 100,
    [int]$BurstDevices = 2000,
    [int]$IntervalMs = 2000
)

$ErrorActionPreference = "Stop"

function Set-DeviceCount($count) {
    $override = @"
services:
  data-ingestion:
    environment:
      BROKER_TYPE: kafka
      KAFKA_ACKS: "1"
      DEVICE_COUNT: "$count"
      INTERVAL_MS: "$IntervalMs"
  analytics:
    environment:
      BROKER_TYPE: kafka
  data-storage:
    environment:
      Broker__Mode: kafka
"@
    $override | Out-File -Encoding utf8 docker-compose.override.yml
    docker compose up -d --build data-ingestion analytics data-storage | Out-Null
}

function Get-RowCount {
    (docker exec iot-postgres psql -U postgres -d iot_db -t -A -c "SELECT COUNT(*) FROM sensor_data;").Trim()
}

function Get-ConsumerLag {
    $output = docker exec iot-kafka /opt/kafka/bin/kafka-consumer-groups.sh `
        --bootstrap-server localhost:9092 --describe --group data-storage-group 2>$null
    $total = 0
    foreach ($line in $output) {
        if ($line -match '^\S+\s+\S+\s+\d+\s+\d+\s+\d+\s+(\d+)\s') {
            $total += [int]$Matches[1]
        }
    }
    return $total
}

Write-Host "=== Scenario C: Burst $BaselineDevices -> $BurstDevices -> $BaselineDevices ===" -ForegroundColor Cyan

# FAZA 1: Baseline
Write-Host ""
Write-Host "--- FAZA 1: Baseline ($BaselineDevices uredjaja) ---" -ForegroundColor Cyan
Set-DeviceCount $BaselineDevices
Start-Sleep -Seconds 10

$A1 = Get-RowCount
$lagBaseline = Get-ConsumerLag
Write-Host "A1 = $A1, Lag (baseline) = $lagBaseline"

Start-Sleep -Seconds 30
$A2 = Get-RowCount
$baselineDelta = [int]$A2 - [int]$A1
Write-Host "A2 = $A2 (Delta baseline = $baselineDelta za 30s)"

# FAZA 2: Burst
Write-Host ""
Write-Host "--- FAZA 2: BURST ($BurstDevices uredjaja) ---" -ForegroundColor Yellow
Set-DeviceCount $BurstDevices
Start-Sleep -Seconds 5  # vreme za rebuild/restart

$lagBurst = Get-ConsumerLag
Write-Host "Lag (odmah nakon burst rebuild-a) = $lagBurst"

Start-Sleep -Seconds 30
$B1 = Get-RowCount
$lagBurstAfter = Get-ConsumerLag
$burstDelta = [int]$B1 - [int]$A2
Write-Host "B1 = $B1 (Delta burst = $burstDelta za ~35s)"
Write-Host "Lag (kraj burst faze) = $lagBurstAfter"

# FAZA 3: Recovery
Write-Host ""
Write-Host "--- FAZA 3: Recovery (vraceno na $BaselineDevices uredjaja) ---" -ForegroundColor Cyan
Set-DeviceCount $BaselineDevices

for ($i = 1; $i -le 4; $i++) {
    Start-Sleep -Seconds 20
    $lag = Get-ConsumerLag
    Write-Host "Recovery provera ${i}/4 (t+$($i*20)s): Consumer Lag = $lag"
}

Write-Host ""
Write-Host "================ REZULTAT ================" -ForegroundColor Green
Write-Host "Baseline ($BaselineDevices uredjaja): Delta=$baselineDelta / 30s, Lag=$lagBaseline"
Write-Host "Burst ($BurstDevices uredjaja):       Delta=$burstDelta / ~35s, Lag pre/posle burst-a=$lagBurst -> $lagBurstAfter"
Write-Host "Recovery: vidi 4 provere lag-a iznad (svaka na 20s)"
