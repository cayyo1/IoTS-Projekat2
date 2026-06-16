# 01-qos-scenario-b.ps1
#
# QoS testiranje (MQTT) + Scenario B (network disconnect 30s) - Sekcija 1
#
# Postavlja sistem u MQTT mod sa zadatim publish QoS-om (ingestion), zatim
# diskonektuje data-storage servis od mreze na 30s i mери da li se poruke
# poslate u tom periodu uspesno isporucuju nakon reconnect-a.
#
# Upotreba:
#   .\01-qos-scenario-b.ps1 -QoS 0
#   .\01-qos-scenario-b.ps1 -QoS 1
#   .\01-qos-scenario-b.ps1 -QoS 2
#
# Pokrece se iz root foldera projekta (gde je docker-compose.yml).
# Preduslov: Docker Desktop pokrenut, ceo stack vec gore (docker compose up -d).

param(
    [Parameter(Mandatory=$true)]
    [ValidateSet(0,1,2)]
    [int]$QoS
)

$ErrorActionPreference = "Stop"

Write-Host "=== QoS test + Scenario B, MQTT_QOS=$QoS ===" -ForegroundColor Cyan

# 1. Override env varijable preko docker-compose.override.yml
$override = @"
services:
  data-ingestion:
    environment:
      BROKER_TYPE: mqtt
      MQTT_QOS: "$QoS"
      DEVICE_COUNT: "3"
      INTERVAL_MS: "2000"
  analytics:
    environment:
      BROKER_TYPE: mqtt
  data-storage:
    environment:
      Broker__Mode: mqtt
      Broker__Mqtt__QoS: "1"
"@
$override | Out-File -Encoding utf8 docker-compose.override.yml

# 2. Rebuild i restart pogodjenih servisa
docker compose up -d --build data-ingestion analytics data-storage
Start-Sleep -Seconds 10

# 3. Detekcija naziva docker mreze
$network = (docker network ls --filter "name=iots-projekat2" --format "{{.Name}}") | Select-Object -First 1
Write-Host "Mreza: $network"

# 4. Helper - broj redova u sensor_data
function Get-RowCount {
    (docker exec iot-postgres psql -U postgres -d iot_db -t -A -c "SELECT COUNT(*) FROM sensor_data;").Trim()
}

# 5. A - pre diskonekcije
$A = Get-RowCount
Write-Host "A (pre diskonekcije) = $A"

# 6. Diskonekcija data-storage na 30s
docker network disconnect $network iot-storage
Write-Host "iot-storage diskonektovan, cekanje 30s..."
Start-Sleep -Seconds 30

# 7. Reconnect + recovery
docker network connect $network iot-storage
Write-Host "iot-storage ponovo povezan, cekanje 15s na recovery..."
Start-Sleep -Seconds 15

# 8. B - posle recovery-ja
$B = Get-RowCount
Write-Host "B (posle recovery-ja) = $B"

$delta = [int]$B - [int]$A

Write-Host ""
Write-Host "================ REZULTAT ================" -ForegroundColor Green
Write-Host "QoS = $QoS"
Write-Host "A (pre)   = $A"
Write-Host "B (posle) = $B"
Write-Host "Delta     = $delta"
Write-Host ""
Write-Host "Logovi iot-storage (traziti 'catch-up' skok u 'Flushed N readings'):" -ForegroundColor Yellow
docker logs iot-storage --tail 20
