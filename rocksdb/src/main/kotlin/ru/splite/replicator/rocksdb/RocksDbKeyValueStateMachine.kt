package ru.splite.replicator.rocksdb

import org.slf4j.LoggerFactory
import ru.splite.replicator.demo.keyvalue.KeyValueCommand
import ru.splite.replicator.demo.keyvalue.KeyValueReply
import ru.splite.replicator.statemachine.ConflictIndex
import ru.splite.replicator.statemachine.ConflictOrderedStateMachine

class RocksDbKeyValueStateMachine(db: RocksDbStore) : ConflictOrderedStateMachine<ByteArray, ByteArray> {

    private val keyValueStore = db.createColumnFamilyStore(KV_COLUMN_FAMILY_NAME)

    override suspend fun apply(command: ByteArray): ByteArray {
        val reply = when (val deserialized: KeyValueCommand = KeyValueCommand.deserializer(command)) {
            is KeyValueCommand.GetValue -> {
                val value = keyValueStore.getAsByteArray(deserialized.key)?.asString()
                KeyValueReply.create(deserialized.key, value)
            }
            is KeyValueCommand.PutValue -> {
                keyValueStore.put(deserialized.key, deserialized.value.toByteArray())
                LOGGER.debug("Set key ${deserialized.key} to value ${deserialized.value}")
                KeyValueReply.create(deserialized.key, deserialized.value)
            }
        }

        return KeyValueReply.serialize(reply)
    }

    override fun <K> newConflictIndex(): ConflictIndex<K, ByteArray> {
        TODO()
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)

        private const val KV_COLUMN_FAMILY_NAME = "KV"

        val COLUMN_FAMILY_NAMES = listOf(
            KV_COLUMN_FAMILY_NAME
        )
    }
}