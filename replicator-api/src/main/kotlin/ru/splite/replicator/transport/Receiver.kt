package ru.splite.replicator.transport

/**
 * Обработчик бинарных сообщений.
 */
interface Receiver {

    /**
     * Идентификатор узла обработчика.
     */
    val address: NodeIdentifier

    /**
     * Вызывается при получении сообщения [payload] от узла [src].
     * @return ответное сообщение
     */
    suspend fun receive(src: NodeIdentifier, payload: ByteArray): ByteArray
}