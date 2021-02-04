package ru.splite.replicator.raft

import ru.splite.replicator.Command
import ru.splite.replicator.LogStoreAssert

fun LogStoreAssert.hasOnlyCommands(vararg values: Long): LogStoreAssert {
    this.hasOnlyEntries(*values.map { Command.Serializer.serialize(Command(it)) }.toTypedArray())
    return this
}