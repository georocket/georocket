#!/bin/bash

set -e

./test-one.sh standalone
./test-one.sh h2
./test-one.sh mongo
./test-one.sh s3
./test-one.sh hdfs
