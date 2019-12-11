#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
# note: 对于 standalone 模式来说，在 JobManager 节点启动，它之后会根据配置远程启动 TM 作业
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

# Start the JobManager instance(s)
# note 不考虑匹配过程中的大小写
shopt -s nocasematch
if [[ $HIGH_AVAILABILITY == "zookeeper" ]]; then
    # HA Mode
    readMasters

    echo "Starting HA cluster with ${#MASTERS[@]} masters."

    for ((i=0;i<${#MASTERS[@]};++i)); do
        master=${MASTERS[i]}
        webuiport=${WEBUIPORTS[i]}

        if [ ${MASTERS_ALL_LOCALHOST} = true ] ; then
            "${FLINK_BIN_DIR}"/jobmanager.sh start "${master}" "${webuiport}"
        else
            ssh -n $FLINK_SSH_OPTS $master -- "nohup /bin/bash -l \"${FLINK_BIN_DIR}/jobmanager.sh\" start ${master} ${webuiport} &"
        fi
    done

else
    echo "Starting cluster."

    # Start single JobManager on this machine
    #note 单机模式下，先启动 job manager
    "$FLINK_BIN_DIR"/jobmanager.sh start
fi
shopt -u nocasematch

# Start TaskManager instance(s)
# note: 这里会根据 slaves 文件配置的机器，将其全部启动（pdsh 能启动远程服务的前提下）
#note 启动 task Manger
TMSlaves start
