package ru.splite.replicator.keyvalue

import org.slf4j.LoggerFactory
import ru.splite.replicator.statemachine.ConflictIndex
import ru.splite.replicator.statemachine.ConflictOrderedStateMachine
import java.util.concurrent.ConcurrentHashMap

class KeyValueStateMachine : ConflictOrderedStateMachine<ByteArray, ByteArray> {

    private val store = ConcurrentHashMap<String, String>()

    val currentState: Map<String, String>
        get() = store

    override fun apply(bytes: ByteArray): ByteArray {
        val reply = when (val command: KeyValueCommand = KeyValueCommand.deserializer(bytes)) {
            is KeyValueCommand.GetValue -> {
                val value = store[command.key]
                KeyValueReply(command.key, value ?: "", value == null)
            }
            is KeyValueCommand.PutValue -> {
                store[command.key] = command.value
                LOGGER.debug("Set key ${command.key} to value ${command.value}")
                KeyValueReply(command.key, command.value, false)
            }
        }

        return KeyValueReply.serialize(reply)
    }

    override fun <K> newConflictIndex(): ConflictIndex<K, ByteArray> {
        return KeyValueConflictIndex()
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}