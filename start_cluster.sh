#!/usr/bin/env bash

set -e

BIN=./build/src/kv_node
CONFIG=./cluster_config.yaml

echo "Starting KV cluster..."

$BIN --id node-1 --port 50051 --config $CONFIG &
sleep 0.3

$BIN --id node-2 --port 50052 --config $CONFIG &
sleep 0.3

$BIN --id node-3 --port 50053 --config $CONFIG &
sleep 0.3

$BIN --id node-4 --port 50054 --config $CONFIG &
sleep 0.3

$BIN --id node-5 --port 50055 --config $CONFIG &
sleep 0.3

wait