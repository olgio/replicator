package ru.splite.replicator.log

interface ReplicatedLogStore {

    fun setLogEntry(index: Long, logEntry: LogEntry)

    fun appendLogEntry(logEntry: LogEntry): Long

    fun getLogEntryByIndex(index: Long): LogEntry?

    fun prune(index: Long): Long

    fun commit(index: Long): Long

    fun lastLogIndex(): Long?

    fun lastCommitIndex(): Long?
}