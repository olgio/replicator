package ru.splite.replicator.transport

/**
 * Внутренний транспорт для обмена бинарными сообщениями.
 */
interface Transport {

    /**
     * Список идентификаторов узлов.
     */
    val nodes: Collection<NodeIdentifier>

    /**
     * Подписка на получение сообщений на идентификаторе [address],
     * используя обработчик сообщений [actor].
     */
    fun subscribe(address: NodeIdentifier, actor: Receiver)

    /**
     * Отправка сообщения [payload] на идентификатор узла [dst].
     * @return ответ на отправленное сообщений
     */
    suspend fun send(receiver: Receiver, dst: NodeIdentifier, payload: ByteArray): ByteArray
}