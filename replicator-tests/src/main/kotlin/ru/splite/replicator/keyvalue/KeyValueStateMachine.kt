package ru.splite.replicator.keyvalue

import org.slf4j.LoggerFactory
import ru.splite.replicator.statemachine.ConflictIndex
import ru.splite.replicator.statemachine.StateMachine
import java.util.concurrent.ConcurrentHashMap

class KeyValueStateMachine : StateMachine<ByteArray, ByteArray> {

    private val store = ConcurrentHashMap<String, String>()

    val currentState: Map<String, String>
        get() = store

    override fun commit(bytes: ByteArray): ByteArray {
        val reply = when (val command: KeyValueCommand = KeyValueCommand.deserializer(bytes)) {
            is KeyValueCommand.GetValue -> {
                KeyValueReply(command.key, store[command.key])
            }
            is KeyValueCommand.PutValue -> {
                store[command.key] = command.value
                LOGGER.debug("Set key ${command.key} to value ${command.value}")
                KeyValueReply(command.key, command.value)
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