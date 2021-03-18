package ru.splite.replicator

fun LogStoreAssert.hasOnlyCommands(vararg values: Long): LogStoreAssert {
    this.hasOnlyEntries(*values.map { Command.serialize(Command(it)) }.toTypedArray())
    return this
}