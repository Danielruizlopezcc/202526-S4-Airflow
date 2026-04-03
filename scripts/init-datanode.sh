#!/bin/bash
set -e

mkdir -p /opt/hadoop/data/dataNode
chmod 755 /opt/hadoop/data/dataNode

echo "Starting DataNode..."
hdfs datanode
