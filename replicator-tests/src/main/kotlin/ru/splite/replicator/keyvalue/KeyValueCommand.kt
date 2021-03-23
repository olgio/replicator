package ru.splite.replicator.keyvalue

import kotlinx.serialization.Serializable
import kotlinx.serialization.protobuf.ProtoBuf
import ru.splite.replicator.transport.CommandSerializer

@Serializable
sealed class KeyValueCommand {

    @Serializable
    data class GetValue(val key: String) : KeyValueCommand()

    @Serializable
    data class PutValue(val key: String, val value: String) : KeyValueCommand()

    companion object Serializer : CommandSerializer<KeyValueCommand> {
        override fun serialize(command: KeyValueCommand): ByteArray {
            return ProtoBuf.encodeToByteArray(serializer(), command)
        }

        override fun deserializer(byteArray: ByteArray): KeyValueCommand {
            return ProtoBuf.decodeFromByteArray(serializer(), byteArray)
        }

        fun newPutCommand(key: String, value: String): ByteArray {
            return serialize(PutValue(key, value))
        }

        fun newGetCommand(key: String): ByteArray {
            return serialize(GetValue(key))
        }
    }
}
