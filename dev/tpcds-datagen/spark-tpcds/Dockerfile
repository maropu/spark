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

# Image to generate TPC-DS data for TPCDSQUeryTestSuite.
# Based on Ubuntu 20.04 to generate the same data with one on GitHub Actions.
#
# Includes:
# * Java 8
#
# You can test it as below:
#   cd dev/tpcds-datagen
#   docker build -t spark-tpcds --build-arg UID=$UID .

FROM ubuntu:20.04

# For apt to be noninteractive
ENV DEBIAN_FRONTEND noninteractive
ENV DEBCONF_NONINTERACTIVE_SEEN true

# These arguments are just for reuse and not really meant to be customized.
ARG APT_INSTALL="apt-get install --no-install-recommends -y"

# This is all in a single "RUN" command so that if anything changes, "apt update" is run to fetch
# the most current package versions (instead of potentially using old versions cached by docker).
RUN apt-get clean && apt-get update && $APT_INSTALL gnupg ca-certificates && \
  gpg --keyserver keyserver.ubuntu.com --recv-key E298A3A825C0D65DFD57CBB651716619E084DAB9 && \
  gpg -a --export E084DAB9 | apt-key add - && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/* && \
  apt-get clean && \
  apt-get update && \
  $APT_INSTALL software-properties-common && \
  apt-get update && \
  # Install openjdk 8.
  $APT_INSTALL openjdk-8-jdk && \
  update-alternatives --set java /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java && \
  # Install build / source control tools
  $APT_INSTALL curl wget git maven subversion make gcc bison flex lsof libffi-dev \
    libssl-dev libcurl4-openssl-dev libxml2-dev && \
  curl -sL https://deb.nodesource.com/setup_12.x | bash && \
  $APT_INSTALL libcurl4-openssl-dev libgit2-dev libssl-dev libxml2-dev

WORKDIR /opt/spark-tpcds/output

ARG UID
RUN useradd -m -s /bin/bash -p spark-tpcds -u $UID spark-tpcds
USER spark-tpcds:spark-tpcds

ENTRYPOINT [ "/opt/spark-tpcds/do-datagen.sh" ]
