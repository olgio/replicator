package ru.splite.replicator.paxos

import ru.splite.replicator.demo.LogStoreAssert

fun assertThatLogs(vararg nodes: PaxosProtocol): LogStoreAssert {
    return LogStoreAssert.assertThatLogs(*nodes.map { it.replicatedLogStore }.toTypedArray())
}

fun assertThatLogs(vararg nodes: PaxosProtocolController): LogStoreAssert {
    return LogStoreAssert.assertThatLogs(*nodes.map { it.protocol.replicatedLogStore }.toTypedArray())
}