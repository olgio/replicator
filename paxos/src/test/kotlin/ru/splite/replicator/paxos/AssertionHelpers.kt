package ru.splite.replicator.paxos

import ru.splite.replicator.LogStoreAssert

fun assertThatLogs(vararg nodes: PaxosProtocol): LogStoreAssert {
    return LogStoreAssert.assertThatLogs(*nodes.map { it.replicatedLogStore }.toTypedArray())
}