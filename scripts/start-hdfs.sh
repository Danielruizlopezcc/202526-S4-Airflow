#!/bin/bash
set -e

mkdir -p /opt/hadoop/data/nameNode
chmod 755 /opt/hadoop/data/nameNode

# Format NameNode only on first boot
if [ ! -d "/opt/hadoop/data/nameNode/current" ]; then
    echo "Formatting NameNode for the first time..."
    echo 'Y' | hdfs namenode -format
fi

echo "Starting NameNode..."
hdfs namenode
