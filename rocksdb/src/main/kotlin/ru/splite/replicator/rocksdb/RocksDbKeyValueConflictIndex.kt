package ru.splite.replicator.rocksdb

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.Serializable
import org.slf4j.LoggerFactory
import ru.splite.replicator.atlas.graph.Dependency
import ru.splite.replicator.demo.keyvalue.KeyValueCommand
import ru.splite.replicator.statemachine.ConflictIndex
import java.util.concurrent.ConcurrentHashMap

class RocksDbKeyValueConflictIndex(db: RocksDbStore) : ConflictIndex<Dependency, ByteArray> {

    private val conflictIndexStore = db.createColumnFamilyStore(CONFLICT_INDEX_COLUMN_FAMILY_NAME)

    @Serializable
    private data class LastReadWrite(
        val lastRead: Dependency? = null,
        val lastWrite: Dependency? = null
    )

    private val lastReadWrite = ConcurrentHashMap(readLastReadWrite())

    private val conflictIndexLock = Mutex()

    override suspend fun putAndGetConflicts(key: Dependency, command: ByteArray): Set<Dependency> =
        conflictIndexLock.withLock {
            return when (val deserializedCommand = KeyValueCommand.deserializer(command)) {
                is KeyValueCommand.GetValue -> {
                    //read depends on last write
                    val oldValue = lastReadWrite[deserializedCommand.key]
                    val newValue = if (oldValue == null) {
                        LastReadWrite(lastRead = key)
                    } else {
                        LastReadWrite(lastRead = key, lastWrite = oldValue.lastWrite)
                    }
                    conflictIndexStore.putAsType(deserializedCommand.key, newValue, LastReadWrite.serializer())
                    lastReadWrite[deserializedCommand.key] = newValue

                    setOfNotNull(newValue.lastWrite)
                }
                is KeyValueCommand.PutValue -> {
                    //write depends on last write and last read
                    val oldValue = lastReadWrite[deserializedCommand.key]

                    var prevLastWrite: Dependency? = null
                    val newValue = if (oldValue == null) {
                        LastReadWrite(lastWrite = key)
                    } else {
                        prevLastWrite = oldValue.lastWrite
                        LastReadWrite(lastRead = oldValue.lastRead, lastWrite = key)
                    }
                    conflictIndexStore.putAsType(deserializedCommand.key, newValue, LastReadWrite.serializer())
                    lastReadWrite[deserializedCommand.key] = newValue
                    setOfNotNull(prevLastWrite, newValue.lastRead)
                }
            }
        }

    private fun readLastReadWrite(): Map<String, LastReadWrite> =
        conflictIndexStore.getAll(LastReadWrite.serializer()).map {
            it.key to it.value
        }.toMap()

    companion object {
        private val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)

        private const val CONFLICT_INDEX_COLUMN_FAMILY_NAME = "KV_CONFLICT_INDEX"

        val COLUMN_FAMILY_NAMES = listOf(
            CONFLICT_INDEX_COLUMN_FAMILY_NAME
        )
    }
}