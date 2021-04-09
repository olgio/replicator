package ru.splite.replicator

import ru.splite.replicator.demo.Command
import ru.splite.replicator.demo.LogStoreAssert

fun LogStoreAssert.hasOnlyCommands(vararg values: Long): LogStoreAssert {
    this.hasOnlyEntries(*values.map { Command.serialize(Command(it)) }.toTypedArray())
    return this
}