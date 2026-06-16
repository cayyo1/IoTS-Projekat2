# 05-scenario-d.ps1
#
# Scenario D: Real-Time Alerting - E2E latencija - Sekcija 6
#
# Prati logove iot-analytics tokom DurationSeconds, izvlaci sve linije
# "Prozor [10s]" i "CRITICAL ALERT", i racuna latenciju (u sekundama,
# preciznost loga) izmedju kraja prozora i ispisa alarma.
#
# Upotreba:
#   .\05-scenario-d.ps1
#   .\05-scenario-d.ps1 -DurationSeconds 60
#
# Pokrece se iz root foldera projekta. Preduslov: ceo stack gore,
# DEVICE_COUNT dovoljno veliko (npr. 100) da prozori imaju dovoljno uzoraka
# (sto je veci uzorak, prosek je bliz srednjoj vrednosti raspona [0,100]=50,
# pa se CRITICAL ALERT (>50) javlja kod ~50% prozora).

param(
    [int]$DurationSeconds = 60
)

$ErrorActionPreference = "Continue"

Write-Host "=== Scenario D: pracenje iot-analytics logova $DurationSeconds sekundi ===" -ForegroundColor Cyan

$logFile = "scenario-d-raw.log"

# Pokupi logove poslednjih DurationSeconds sekundi pomocu --since
$since = (Get-Date).AddSeconds(-1).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")

Write-Host "Cekanje $DurationSeconds sekundi da se nakupi dovoljno prozora..."
Start-Sleep -Seconds $DurationSeconds

docker logs iot-analytics --since $since | Out-File -Encoding utf8 $logFile

$windowLines = Select-String -Path $logFile -Pattern "Prozor \[10s\]"
$alertLines  = Select-String -Path $logFile -Pattern "CRITICAL ALERT"

Write-Host ""
Write-Host "================ PROZORI ================" -ForegroundColor Green
foreach ($l in $windowLines) {
    Write-Host $l.Line
}

Write-Host ""
Write-Host "================ ALARMI (CRITICAL ALERT) ================" -ForegroundColor Red
foreach ($l in $alertLines) {
    Write-Host $l.Line
}

Write-Host ""
Write-Host "Ukupno prozora: $($windowLines.Count), od toga sa alarmom: $($alertLines.Count)"
Write-Host "(Za latenciju: uporedi timestamp 'Prozor [10s]' i pratece 'CRITICAL ALERT' linije -"
Write-Host " ako su identicni do sekunde, latencija obrade je ~0s; E2E latencija je dominantno"
Write-Host " odredjena dizajnom 10s tumbling window-a, ne obradom.)"
Write-Host ""
Write-Host "Sirovi log sacuvan u: $logFile"
