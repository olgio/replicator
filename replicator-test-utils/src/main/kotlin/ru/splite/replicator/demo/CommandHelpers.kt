package ru.splite.replicator.demo

suspend fun LogStoreAssert.hasOnlyCommands(vararg values: Long): LogStoreAssert {
    this.hasOnlyEntries(*values.map { Command.serialize(Command(it)) }.toTypedArray())
    return this
}