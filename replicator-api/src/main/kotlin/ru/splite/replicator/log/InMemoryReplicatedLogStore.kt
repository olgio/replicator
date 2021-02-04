package ru.splite.replicator.log

import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

class InMemoryReplicatedLogStore : ReplicatedLogStore {

    private val logEntries: MutableMap<Long, LogEntry> = ConcurrentHashMap()

    private val lastIndex = AtomicLong(-1)

    private val lastCommitIndex = AtomicLong(-1)

    override fun setLogEntry(index: Long, logEntry: LogEntry) {
        validateIndex(index)
        if (lastCommitIndex.get() >= index) {
            error("Cannot override committed log entry at index $index")
        }
        logEntries[index] = logEntry
        lastIndex.set(index)
        LOGGER.info("Set log entry with index $index: $logEntry")
    }

    override fun appendLogEntry(logEntry: LogEntry): Long {
        val newIndex = lastIndex.incrementAndGet()
        logEntries[newIndex] = logEntry
        LOGGER.info("Appended log with index $newIndex: $logEntry")
        return newIndex
    }

    override fun getLogEntryByIndex(index: Long): LogEntry? {
        validateIndex(index)
        if (index > lastIndex.get()) {
            return null
        }
        return logEntries[index]
    }

    override fun prune(index: Long): Long {
        validateIndex(index)
        if (lastCommitIndex.get() >= index) {
            error("Cannot prune log because lastCommitIndex ${lastCommitIndex.get()} >= $index")
        }
        lastIndex.set(index - 1)
        return 0L
    }

    override fun commit(index: Long): Long {
        validateIndex(index)
        if (index > lastIndex.get()) {
            error("Cannot commit log with gaps: $index > ${lastIndex.get()}")
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

    private fun validateIndex(index: Long) {
        if (index < 0) {
            error("Log entry index cannot be less than 0")
        }
    }

    companion object {
        val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}