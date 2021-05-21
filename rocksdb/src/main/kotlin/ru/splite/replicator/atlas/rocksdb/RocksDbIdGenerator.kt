package ru.splite.replicator.atlas.rocksdb

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import ru.splite.replicator.atlas.id.Id
import ru.splite.replicator.atlas.id.IdGenerator
import ru.splite.replicator.rocksdb.RocksDbStore
import ru.splite.replicator.rocksdb.asLong
import ru.splite.replicator.rocksdb.toByteArray
import java.util.concurrent.atomic.AtomicLong

class RocksDbIdGenerator<S>(
    private val node: S,
    db: RocksDbStore,
    private val reservedIdStep: Int = 200
) : IdGenerator<S> {

    private val idSequenceStore = db.createColumnFamilyStore(ID_SEQUENCE_COLUMN_FAMILY_NAME)

    private val incrementReservedLock = Mutex()

    private val lastReservedId = runBlocking {
        AtomicLong(readLastReservedId())
    }

    private val lastId = AtomicLong(lastReservedId.get())

    override suspend fun generateNext(): Id<S> {
        return Id(node, generateNextId())
    }

    private suspend fun generateNextId(): Long {
        while (true) {
            val oldValue = lastId.get()
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
            val newValue = oldValue + 1
            if (lastId.compareAndSet(oldValue, newValue)) {
                return newValue
            }
        }
    }

    private suspend fun readLastReservedId(): Long = idSequenceStore
        .getAsByteArray(ID_SEQUENCE_KEY)?.asLong() ?: 0L


    companion object {
        private const val ID_SEQUENCE_COLUMN_FAMILY_NAME = "ID_SEQUENCE"
        private const val ID_SEQUENCE_KEY = "ID"

        val COLUMN_FAMILY_NAMES = listOf(
            ID_SEQUENCE_COLUMN_FAMILY_NAME
        )
    }
}