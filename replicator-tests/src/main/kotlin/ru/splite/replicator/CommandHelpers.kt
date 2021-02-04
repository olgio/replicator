package ru.splite.replicator.raft

import kotlinx.serialization.encodeToByteArray
import kotlinx.serialization.protobuf.ProtoBuf
import ru.splite.replicator.Command
import ru.splite.replicator.LogStoreAssert

fun LogStoreAssert.hasOnlyCommands(vararg values: Long): LogStoreAssert {
    this.hasOnlyEntries(*values.map { ProtoBuf.encodeToByteArray(Command(it)) }.toTypedArray())
    return this
}