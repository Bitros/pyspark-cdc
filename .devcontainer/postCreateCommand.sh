#!/usr/bin/env bash

python -m pip install -U pip poetry

poetry lock --regenerate
poetry install
poetry run pre-commit autoupdate
poetry run pre-commit install
poetry run pre-commit install-hooks
