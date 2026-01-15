# This script takes care of running the pipeline on windows platform

Write-Host "Activating environment..."
conda activate subs-canc

Write-Host "Running data pipeline..."
python -m src.run_pipeline

Write-Host "Pipeline completed successfully"
