param()

$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)

$streamlitConfigDir = Join-Path $env:USERPROFILE ".streamlit"
New-Item -ItemType Directory -Path $streamlitConfigDir -Force | Out-Null
Set-Content -Path (Join-Path $streamlitConfigDir "credentials.toml") -Value @("[general]", 'email = ""')
$env:STREAMLIT_BROWSER_GATHER_USAGE_STATS = "false"
$env:STREAMLIT_SERVER_HEADLESS = "true"
$env:DATA_DIR = (Join-Path (Get-Location) "data\sample")

Write-Host "Subindo demo com base amostral em http://localhost:8501 ..."
streamlit run services/dashboard/app.py --server.headless true --browser.gatherUsageStats false
