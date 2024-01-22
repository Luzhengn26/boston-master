#!/usr/bin/env bash
set -e

make build
docker run --rm -v $(pwd):/boston -w /boston/src boston:build
