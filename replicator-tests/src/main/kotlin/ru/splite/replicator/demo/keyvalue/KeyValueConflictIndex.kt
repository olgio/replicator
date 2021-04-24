package ru.splite.replicator.demo.keyvalue

import ru.splite.replicator.statemachine.ConflictIndex
import java.util.concurrent.ConcurrentHashMap

internal class KeyValueConflictIndex<K> : ConflictIndex<K, ByteArray> {

    private data class LastReadWrite<K>(val lastRead: K? = null, val lastWrite: K? = null)

    private val lastReadWrite = ConcurrentHashMap<String, LastReadWrite<K>>()

    override fun putAndGetConflicts(key: K, command: ByteArray): Set<K> {
        return when (val deserializedCommand = KeyValueCommand.deserializer(command)) {
            is KeyValueCommand.GetValue -> {
                //read depends on last write
                val newValue = lastReadWrite.compute(deserializedCommand.key) { _, oldValue ->
                    if (oldValue == null) {
                        LastReadWrite(lastRead = key)
                    } else {
                        LastReadWrite(lastRead = key, lastWrite = oldValue.lastWrite)
                    }
                }
                setOfNotNull(newValue!!.lastWrite)
            }
            is KeyValueCommand.PutValue -> {
                //write depends on last write and last read
                var prevLastWrite: K? = null
                val newValue = lastReadWrite.compute(deserializedCommand.key) { _, oldValue ->
                    if (oldValue == null) {
                        LastReadWrite(lastWrite = key)
                    } else {
                        prevLastWrite = oldValue.lastWrite
                        LastReadWrite(lastRead = oldValue.lastRead, lastWrite = key)
                    }
                }
                setOfNotNull(prevLastWrite, newValue!!.lastRead)
            }
        }
    }
}