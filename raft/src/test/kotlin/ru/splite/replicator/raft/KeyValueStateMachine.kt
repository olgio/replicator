package ru.splite.replicator.raft

import kotlinx.serialization.Serializable
import kotlinx.serialization.decodeFromByteArray
import kotlinx.serialization.encodeToByteArray
import kotlinx.serialization.protobuf.ProtoBuf
import ru.splite.replicator.statemachine.StateMachine
import ru.splite.replicator.transport.CommandSerializer
import java.util.concurrent.ConcurrentHashMap

class KeyValueStateMachine : StateMachine<ByteArray, ByteArray> {

    private val store = ConcurrentHashMap<String, String>()

    override fun commit(bytes: ByteArray): ByteArray {
        val command: PutCommand = deserializer(bytes)
        store[command.key] = command.value
        return bytes
    }

    @Serializable
    data class PutCommand(val key: String, val value: String)

    companion object Serializer : CommandSerializer<PutCommand> {
        override fun serialize(command: PutCommand): ByteArray {
            return ProtoBuf.encodeToByteArray(command)
        }

        override fun deserializer(byteArray: ByteArray): PutCommand {
            return ProtoBuf.decodeFromByteArray<PutCommand>(byteArray)
        }
    }
}