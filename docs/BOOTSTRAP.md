# Bootstrap Rapido

## Objetivo

Subir o cockpit rapidamente com uma base amostral versionada, sem depender da execucao completa do pipeline.

## Demo Local

No Windows PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_demo.ps1
```

Esse fluxo:

- aponta o app para `data/sample/nowcasting.db`;
- evita o prompt inicial do Streamlit;
- sobe o dashboard diretamente em `http://localhost:8501`.

## Execucao Completa

Para reconstruir a base localmente com dados reais:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_local.ps1
```

Ou manualmente:

```bash
python scripts/init_db.py
python scripts/test_pipeline.py
streamlit run services/dashboard/app.py --server.headless true --browser.gatherUsageStats false
```
