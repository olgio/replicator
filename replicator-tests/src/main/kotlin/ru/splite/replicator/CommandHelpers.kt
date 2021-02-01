package ru.splite.replicator.raft

import ru.splite.replicator.Command
import ru.splite.replicator.LogStoreAssert

fun LogStoreAssert<Command>.hasOnlyCommands(vararg values: Long): LogStoreAssert<Command> {
    this.hasOnlyEntries(*values.map { Command(it) }.toTypedArray())
    return this
}