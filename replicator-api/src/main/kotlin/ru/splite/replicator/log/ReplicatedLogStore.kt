package ru.splite.replicator.log

interface ReplicatedLogStore<C> {

    fun setLogEntry(index: Long, logEntry: LogEntry<C>)

    fun appendLogEntry(logEntry: LogEntry<C>): Long

    fun getLogEntryByIndex(index: Long): LogEntry<C>?

    fun prune(index: Long): Long

    fun commit(index: Long): Long

    fun lastLogIndex(): Long?

    fun lastCommitIndex(): Long?
}