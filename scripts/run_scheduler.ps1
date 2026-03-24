param(
    [int]$Cycles = 0,
    [int]$IntervalSeconds = 3600,
    [switch]$RunTestsAfterEachCycle
)

$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)

$cycle = 0
while ($true) {
    $cycle += 1
    Write-Host "[$(Get-Date -Format s)] Iniciando ciclo $cycle"
    python scripts/init_db.py
    python scripts/test_pipeline.py

    if ($RunTestsAfterEachCycle) {
        python -m unittest discover -s tests -v
    }

    if ($Cycles -gt 0 -and $cycle -ge $Cycles) {
        break
    }

    Write-Host "[$(Get-Date -Format s)] Aguardando $IntervalSeconds segundos para o proximo ciclo"
    Start-Sleep -Seconds $IntervalSeconds
}
