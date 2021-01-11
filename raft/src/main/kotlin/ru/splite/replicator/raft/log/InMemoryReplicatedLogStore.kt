package ru.splite.replicator.raft.log

import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

class InMemoryReplicatedLogStore<C> : ReplicatedLogStore<C> {

    private val logEntries: MutableMap<Long, LogEntry<C>> = ConcurrentHashMap()

    private val lastIndex = AtomicLong(-1)

    private val lastCommitIndex = AtomicLong(-1)

    override fun setLogEntry(index: Long, logEntry: LogEntry<C>) {
        logEntries[index] = logEntry
        lastIndex.set(index)
        LOGGER.info("Set log entry with index $index: $logEntry")
    }

    override fun appendLogEntry(logEntry: LogEntry<C>): Long {
        val newIndex = lastIndex.incrementAndGet()
        logEntries[newIndex] = logEntry
        LOGGER.info("Appended log with index $newIndex: $logEntry")
        return newIndex
    }

    override fun getLogEntryByIndex(index: Long): LogEntry<C>? {
        if (index > lastIndex.get()) {
            return null
        }
        return logEntries[index]
    }

    override fun prune(index: Long): Long {
        lastIndex.set(index - 1)
        return 0L
    }

    override fun commit(index: Long): Long {
        checkNotEmpty()
        if (index > lastIndex.get()) {
            error("$index > ${lastIndex.get()}")
        }
        LOGGER.info("Committed command with index {}: {}", index, this.getLogEntryByIndex(index))
        lastCommitIndex.set(index)
        return 0L
    }

    override fun lastLogIndex(): Long? {
        lastIndex.get().let {
            return if (it < 0) null else it
        }
    }

    override fun lastCommitIndex(): Long? {
        lastCommitIndex.get().let {
            return if (it < 0) null else it
        }
    }

    private fun checkNotEmpty() {
        if (lastIndex.get() < 0) {
            error("Log is empty")
        }
    }

    companion object {
        val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}