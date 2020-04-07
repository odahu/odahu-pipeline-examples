#!/usr/bin/env bash

set -e

cp -r manifests "$DAGS_FOLDER/"
cd pipeline
zip -r ../dags/reuters.zip ./*
cd -
cp -r dags/* "$DAGS_FOLDER/"


