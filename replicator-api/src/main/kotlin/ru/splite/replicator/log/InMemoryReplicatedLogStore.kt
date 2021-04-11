package ru.splite.replicator.log

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.max

class InMemoryReplicatedLogStore : ReplicatedLogStore {

    private val logEntries: MutableMap<Long, LogEntry> = ConcurrentHashMap()

    private val lastIndex = AtomicLong(-1)

    private val lastCommitIndex = AtomicLong(-1)

    override fun setLogEntry(index: Long, logEntry: LogEntry) {
        validateIndex(index)
        if (lastCommitIndex.get() >= index) {
            throw CommittedLogEntryOverrideException(
                "Cannot override committed log entry at index $index"
            )
        }
        val firstFreeIndex = lastIndex.get() + 1
        if (index > firstFreeIndex) {
            throw LogGapException(
                "Cannot override log entry as this will cause a gap in the log"
            )
        } else if (index == firstFreeIndex) {
            lastIndex.set(index)
        }
        logEntries[index] = logEntry
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

    override fun prune(index: Long): Long? {
        validateIndex(index)
        if (lastCommitIndex.get() >= index) {
            throw CommittedLogEntryOverrideException(
                "Cannot prune log because lastCommitIndex ${lastCommitIndex.get()} >= $index"
            )
        }
        val newIndex = index - 1
        lastIndex.set(newIndex)
        return if (newIndex < 0) null else newIndex
    }

    override fun commit(index: Long): Long {
        validateIndex(index)
        if (index > lastIndex.get()) {
            throw LogGapException(
                "Cannot commit log with gaps: $index > ${lastIndex.get()}"
            )
        }
        val newIndex = lastCommitIndex.updateAndGet { oldIndex -> max(oldIndex, index) }
        LOGGER.info("Committed command with index {}: {}", index, this.getLogEntryByIndex(index))
        return newIndex
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
        val LOGGER: Logger = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}