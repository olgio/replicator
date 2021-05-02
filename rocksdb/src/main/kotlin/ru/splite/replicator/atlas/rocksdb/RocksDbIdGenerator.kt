package ru.splite.replicator.atlas.rocksdb

import ru.splite.replicator.atlas.id.Id
import ru.splite.replicator.atlas.id.IdGenerator
import ru.splite.replicator.rocksdb.RocksDbStore
import ru.splite.replicator.rocksdb.asLong
import ru.splite.replicator.rocksdb.toByteArray
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class RocksDbIdGenerator<S>(
    private val node: S,
    db: RocksDbStore,
    private val reservedIdStep: Int = 200
) : IdGenerator<S> {

    private val idSequenceStore = db.createColumnFamilyStore(ID_SEQUENCE_COLUMN_FAMILY_NAME)

    private val incrementReservedLock = ReentrantLock()

    private val lastReservedId = AtomicLong(readLastReservedId())

    private val lastId = AtomicLong(lastReservedId.get())

    override suspend fun generateNext(): Id<S> {
        return Id(node, generateNextId())
    }

    private fun generateNextId(): Long {
        return lastId.updateAndGet { oldValue ->
            if (oldValue >= lastReservedId.get()) {
                incrementReservedLock.withLock {
                    val currentReservedId = lastReservedId.get()
                    if (oldValue >= currentReservedId) {
                        val newReservedId = currentReservedId + reservedIdStep
                        idSequenceStore.put(ID_SEQUENCE_KEY, newReservedId.toByteArray())
                        lastReservedId.set(newReservedId)
                    }
                }
            }
            oldValue + 1
        }
    }

    private fun readLastReservedId(): Long = idSequenceStore
        .getAsByteArray(ID_SEQUENCE_KEY)?.asLong() ?: 0L


    companion object {
        private const val ID_SEQUENCE_COLUMN_FAMILY_NAME = "ID_SEQUENCE"
        private const val ID_SEQUENCE_KEY = "ID"

        val COLUMN_FAMILY_NAMES = listOf(
            ID_SEQUENCE_COLUMN_FAMILY_NAME
        )
    }
}