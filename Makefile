
DAGS_FOLDER := ""
CONDA_ENVS := /Users/vlad/opt/miniconda3/envs

-include .env

.EXPORT_ALL_VARIABLES:
.ONESHELL:

sync_dags:
	./scripts/sync_local_dags.sh
