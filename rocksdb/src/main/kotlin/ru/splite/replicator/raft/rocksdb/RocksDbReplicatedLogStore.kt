package ru.splite.replicator.raft.rocksdb

import com.google.common.primitives.Longs
import kotlinx.coroutines.runBlocking
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

    private val lastIndex = runBlocking {
        AtomicLong(readIndexByKey(LAST_INDEX_KEY))
    }

    private val lastCommitIndex = runBlocking {
        AtomicLong(readIndexByKey(COMMIT_INDEX_KEY))
    }

    private val lastAppliedIndex = runBlocking {
        AtomicLong(readIndexByKey(APPLIED_INDEX_KEY))
    }

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

    override suspend fun markApplied(index: Long): Long {
        validateIndex(index)
        if (index > lastCommitIndex.get()) {
            throw LogGapException(
                "Cannot mark applied because command not committed: $index > ${lastCommitIndex.get()}"
            )
        }
        val newIndex = max(lastAppliedIndex.get(), index)
        putAppliedIndex(newIndex)
        return newIndex
    }

    override suspend fun getLogEntryByIndex(index: Long): LogEntry? {
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

    override fun lastAppliedIndex(): Long? {
        lastAppliedIndex.get().let {
            return if (it < 0) null else it
        }
    }

    private fun validateIndex(index: Long) {
        if (index < 0) {
            error("Log entry index cannot be less than 0")
        }
    }

    private suspend fun readIndexByKey(storeKey: String): Long {
        return indexStore.getAsByteArray(storeKey)?.asLong() ?: -1L
    }

    private suspend fun putLastLogIndex(index: Long) {
        indexStore.put(LAST_INDEX_KEY, Longs.toByteArray(index))
        lastIndex.set(index)
    }

    private suspend fun putCommitIndex(index: Long) {
        indexStore.put(COMMIT_INDEX_KEY, Longs.toByteArray(index))
        lastCommitIndex.set(index)
    }

    private suspend fun putAppliedIndex(index: Long) {
        indexStore.put(APPLIED_INDEX_KEY, Longs.toByteArray(index))
        lastAppliedIndex.set(index)
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(javaClass.enclosingClass)

        private const val REPLICATED_LOG_COLUMN_FAMILY_NAME = "REPLICATED_LOG"

        private const val LOG_INDEX_FAMILY_NAME = "LOG_INDEX"
        private const val LAST_INDEX_KEY = "LAST_INDEX"
        private const val COMMIT_INDEX_KEY = "COMMIT_INDEX"
        private const val APPLIED_INDEX_KEY = "APPLIED_INDEX"

        val COLUMN_FAMILY_NAMES = listOf(
            LOG_INDEX_FAMILY_NAME,
            REPLICATED_LOG_COLUMN_FAMILY_NAME
        )
    }
}