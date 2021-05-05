package ru.splite.replicator.atlas.id

/**
 * Генерация идентификаторов команд
 */
interface IdGenerator<S> {

    /**
     * Генерация нового идентификатора команды
     */
    suspend fun generateNext(): Id<S>
}