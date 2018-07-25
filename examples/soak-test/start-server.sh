#!/usr/bin/env bash

HAZELCAST_TEST_VERSION="3.12.5"
HAZELCAST_VERSION="3.12.5"

CLASSPATH="hazelcast-${HAZELCAST_VERSION}.jar:hazelcast-${HAZELCAST_TEST_VERSION}-tests.jar"
CMD_CONFIGS="-Dhazelcast.multicast.group=224.206.1.1 -Djava.net.preferIPv4Stack=true"
java ${CMD_CONFIGS} -cp ${CLASSPATH} com.hazelcast.core.server.StartServer > hazelcast-${HAZELCAST_VERSION}-out-${1}.log 2>hazelcast-${HAZELCAST_VERSION}-err-${1}.log &