package ru.splite.replicator.transport

interface CommandSerializer<C> {

    fun serialize(command: C): ByteArray

    fun deserializer(byteArray: ByteArray): C
}