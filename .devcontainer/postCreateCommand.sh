#!/usr/bin/env bash

python -m pip install -U pip poetry
poetry install
poetry env activate


code --uninstall-extension vscjava.vscode-java-pack
