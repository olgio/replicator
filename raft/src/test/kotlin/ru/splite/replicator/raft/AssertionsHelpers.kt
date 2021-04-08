package ru.splite.replicator.raft

import ru.splite.replicator.LogStoreAssert
import ru.splite.replicator.raft.cluster.RaftClusterNode

fun assertThatLogs(vararg nodes: RaftProtocol): LogStoreAssert {
    return LogStoreAssert.assertThatLogs(*nodes.map { it.replicatedLogStore }.toTypedArray())
}

fun assertThatLogs(vararg nodes: RaftClusterNode): LogStoreAssert {
    return LogStoreAssert.assertThatLogs(*nodes.map { it.logStore }.toTypedArray())
}

fun assertThatLogs(vararg nodes: RaftProtocolController): LogStoreAssert {
    return LogStoreAssert.assertThatLogs(*nodes.map { it.protocol.replicatedLogStore }.toTypedArray())
}