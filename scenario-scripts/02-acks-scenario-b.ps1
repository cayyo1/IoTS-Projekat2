# 02-acks-scenario-b.ps1
#
# acks testiranje (Kafka) + Scenario B (network disconnect 30s) + Consumer Lag
# - Sekcija 2
#
# Postavlja sistem u Kafka mod sa zadatim producer acks nivoom, diskonektuje
# data-storage na 30s, i prati broj redova u sensor_data + Consumer Lag
# (grupa data-storage-group) pre / tokom / posle diskonekcije.
#
# Upotreba:
#   .\02-acks-scenario-b.ps1 -Acks 0
#   .\02-acks-scenario-b.ps1 -Acks 1
#   .\02-acks-scenario-b.ps1 -Acks -1     # acks=all
#
# Pokrece se iz root foldera projekta. Preduslov: Docker Desktop pokrenut,
# ceo stack vec gore.

param(
    [Parameter(Mandatory=$true)]
    [ValidateSet(0,1,-1)]
    [int]$Acks
)

$ErrorActionPreference = "Stop"

Write-Host "=== acks test + Scenario B + Consumer Lag, KAFKA_ACKS=$Acks ===" -ForegroundColor Cyan

# 1. Override env varijable
$override = @"
services:
  data-ingestion:
    environment:
      BROKER_TYPE: kafka
      KAFKA_ACKS: "$Acks"
      DEVICE_COUNT: "3"
      INTERVAL_MS: "2000"
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

# 3. Mreza
$network = (docker network ls --filter "name=iots-projekat2" --format "{{.Name}}") | Select-Object -First 1
Write-Host "Mreza: $network"

# 4. Helperi
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

# 5. PRE diskonekcije
$A = Get-RowCount
$lagPre = Get-ConsumerLag
Write-Host "A (pre diskonekcije) = $A, Consumer Lag (pre) = $lagPre"

# 6. Diskonekcija
docker network disconnect $network iot-storage
Write-Host "iot-storage diskonektovan, cekanje 30s..."
Start-Sleep -Seconds 30

# 7. TOKOM diskonekcije (lag se cita sa iot-kafka strane, radi i kad je
#    iot-storage offline)
$lagDuring = Get-ConsumerLag
Write-Host "Consumer Lag (tokom diskonekcije) = $lagDuring"

# 8. Reconnect + recovery
docker network connect $network iot-storage
Write-Host "iot-storage ponovo povezan, cekanje 15s na recovery..."
Start-Sleep -Seconds 15

# 9. POSLE recovery-ja
$B = Get-RowCount
$lagPost = Get-ConsumerLag
Write-Host "B (posle recovery-ja) = $B, Consumer Lag (posle) = $lagPost"

$delta = [int]$B - [int]$A

Write-Host ""
Write-Host "================ REZULTAT ================" -ForegroundColor Green
Write-Host "acks = $Acks"
Write-Host "A (pre)   = $A"
Write-Host "B (posle) = $B"
Write-Host "Delta     = $delta"
Write-Host "Consumer Lag: pre=$lagPre, tokom=$lagDuring, posle=$lagPost"
Write-Host ""
Write-Host "Logovi iot-storage:" -ForegroundColor Yellow
docker logs iot-storage --tail 20
