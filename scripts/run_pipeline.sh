#!/usr/bin/env bash

# Run the data pipeline end to end on Unix or Unix-Like platform

set -e

echo "Activating environment..."
conda activate subs-canc

echo "Running data pipeline..."
python -m src.run_pipeline

echo "Pipeline completed successfully"
