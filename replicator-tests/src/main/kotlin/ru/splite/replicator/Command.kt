package ru.splite.replicator

import kotlinx.serialization.Serializable
import kotlinx.serialization.decodeFromByteArray
import kotlinx.serialization.encodeToByteArray
import kotlinx.serialization.protobuf.ProtoBuf
import ru.splite.replicator.transport.CommandSerializer

@Serializable
data class Command(val value: Long) {

    companion object Serializer : CommandSerializer<Command> {
        override fun serialize(command: Command): ByteArray {
            return ProtoBuf.encodeToByteArray(command)
        }

        override fun deserializer(byteArray: ByteArray): Command {
            return ProtoBuf.decodeFromByteArray(byteArray)
        }
    }
}