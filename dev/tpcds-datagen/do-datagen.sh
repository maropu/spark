#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e
SELF=$(cd $(dirname $0) && pwd)

# Re-uses helper funcs for the release scripts
if [ "$RUNNING_IN_DOCKER" = "1" ]; then
  . "$SELF/release-util.sh"
else
  . "$SELF/../create-release/release-util.sh"
fi

export LC_ALL=C.UTF-8
export LANG=C.UTF-8

# Checks out tpcds-kit and builds dsdgen
rm -rf tpcds-kit
git clone https://github.com/databricks/tpcds-kit
cd tpcds-kit/tools
run_silent "Building dsdgen in tpcds-kit..." "$SELF/dsdgen-build.log" make OS=LINUX
cd ../..

# Builds Spark to generate TPC-DS data
if [ -z "$SCALE_FACTOR" ]; then
  SCALE_FACTOR=1
fi

rm -rf spark
# git clone https://github.com/apache/spark
SBT_OPTS="-Xmx2g -XX:ReservedCodeCacheSize=1g -XX:+UseG1GC"
git clone https://github.com/maropu/spark
cd spark
git checkout tpcdsDatagen
./build/sbt "sql/test:runMain org.apache.spark.sql.GenTPCDSData --dsdgenDir $SELF/tpcds-kit/tools --location $SELF/tpcds-data --scaleFactor $SCALE_FACTOR"
# run_silent "Building Spark to generate TPC-DS data in $SELF/tpcds-data..." "$SELF/spark-build.log" \
#   ./build/sbt "sql/test:runMain org.apache.spark.sql.GenTPCDSData --dsdgenDir $SELF/tpcds-kit/tools --location $SELF/tpcds-data --scaleFactor $SCALE_FACTOR"
cd ..

rm -rf spark tpcds-kit
