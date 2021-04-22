package ru.splite.replicator.transport

/**
 * Интерфейс для сериализации сообщений в бинарный вид.
 * @param C тип сообщения
 */
interface CommandSerializer<C> {

    /**
     * Сериализация [command].
     * @return сообщение в бинарном виде
     */
    fun serialize(command: C): ByteArray

    /**
     * Десериализация бинарного сообщения [byteArray].
     * @return сообщение после десериализации
     */
    fun deserializer(byteArray: ByteArray): C
}