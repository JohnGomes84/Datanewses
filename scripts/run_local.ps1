param(
    [switch]$SkipPipeline,
    [switch]$RunTests
)

$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)

$streamlitConfigDir = Join-Path $env:USERPROFILE ".streamlit"
New-Item -ItemType Directory -Path $streamlitConfigDir -Force | Out-Null
Set-Content -Path (Join-Path $streamlitConfigDir "credentials.toml") -Value @("[general]", 'email = ""')
$env:STREAMLIT_BROWSER_GATHER_USAGE_STATS = "false"
$env:STREAMLIT_SERVER_HEADLESS = "true"

Write-Host "Inicializando banco..."
python scripts/init_db.py

if (-not $SkipPipeline) {
    Write-Host "Executando pipeline completo..."
    python scripts/test_pipeline.py
}

if ($RunTests) {
    Write-Host "Executando testes smoke..."
    python -m unittest discover -s tests -v
}

Write-Host "Subindo dashboard em http://localhost:8501 ..."
streamlit run services/dashboard/app.py --server.headless true --browser.gatherUsageStats false
