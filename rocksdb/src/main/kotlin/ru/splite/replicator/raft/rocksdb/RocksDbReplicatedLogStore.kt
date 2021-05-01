package ru.splite.replicator.raft.rocksdb

import com.google.common.primitives.Longs
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.splite.replicator.log.CommittedLogEntryOverrideException
import ru.splite.replicator.log.LogEntry
import ru.splite.replicator.log.LogGapException
import ru.splite.replicator.log.ReplicatedLogStore
import ru.splite.replicator.rocksdb.RocksDbStore
import ru.splite.replicator.rocksdb.asLong
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.max

class RocksDbReplicatedLogStore(db: RocksDbStore) : ReplicatedLogStore {

    private val logStore = db.createColumnFamilyStore(REPLICATED_LOG_COLUMN_FAMILY_NAME)

    private val indexStore = db.createColumnFamilyStore(LOG_INDEX_FAMILY_NAME)

    private val lastIndex = AtomicLong(readLastLogIndex())

    private val lastCommitIndex = AtomicLong(readLastCommitIndex())

    private val logMutex = Mutex()

    override suspend fun setLogEntry(index: Long, logEntry: LogEntry) = logMutex.withLock {
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
            putLastLogIndex(index)
        }
        logStore.putAsType(index, logEntry, LogEntry.serializer())
        LOGGER.info("Set log entry with index $index: $logEntry")
    }

    override suspend fun appendLogEntry(logEntry: LogEntry): Long = logMutex.withLock {
        val newIndex = lastIndex.get() + 1
        logStore.putAsType(newIndex, logEntry, LogEntry.serializer())
        putLastLogIndex(newIndex)
        LOGGER.info("Appended log with index $newIndex: $logEntry")
        return newIndex
    }

    override suspend fun prune(index: Long): Long? {
        validateIndex(index)
        if (lastCommitIndex.get() >= index) {
            throw CommittedLogEntryOverrideException(
                "Cannot prune log because lastCommitIndex ${lastCommitIndex.get()} >= $index"
            )
        }
        val newIndex = index - 1
        putLastLogIndex(newIndex)
        return if (newIndex < 0) null else newIndex
    }

    override suspend fun commit(index: Long): Long = logMutex.withLock {
        validateIndex(index)
        if (index > lastIndex.get()) {
            throw LogGapException(
                "Cannot commit log with gaps: $index > ${lastIndex.get()}"
            )
        }
        val newIndex = max(lastCommitIndex.get(), index)
        putCommitIndex(newIndex)
        LOGGER.info("Committed command with index {}: {}", index, this.getLogEntryByIndex(index))
        return newIndex
    }

    override fun getLogEntryByIndex(index: Long): LogEntry? {
        validateIndex(index)
        if (index > lastIndex.get()) {
            return null
        }
        return logStore.getAsType(index, LogEntry.serializer())
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

    private fun readLastLogIndex(): Long {
        return indexStore.getAsByteArray(LAST_INDEX_KEY)?.asLong() ?: -1L
    }

    private fun readLastCommitIndex(): Long {
        return indexStore.getAsByteArray(COMMIT_INDEX_KEY)?.asLong() ?: -1L
    }

    private fun putLastLogIndex(index: Long) {
        indexStore.put(LAST_INDEX_KEY, Longs.toByteArray(index))
        lastIndex.set(index)
    }

    private fun putCommitIndex(index: Long) {
        indexStore.put(COMMIT_INDEX_KEY, Longs.toByteArray(index))
        lastCommitIndex.set(index)
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(javaClass.enclosingClass)

        private const val REPLICATED_LOG_COLUMN_FAMILY_NAME = "REPLICATED_LOG"

        private const val LOG_INDEX_FAMILY_NAME = "LOG_INDEX"
        private const val LAST_INDEX_KEY = "LAST_INDEX"
        private const val COMMIT_INDEX_KEY = "COMMIT_INDEX"

        val COLUMN_FAMILY_NAMES = listOf(
            LOG_INDEX_FAMILY_NAME,
            REPLICATED_LOG_COLUMN_FAMILY_NAME
        )
    }
}