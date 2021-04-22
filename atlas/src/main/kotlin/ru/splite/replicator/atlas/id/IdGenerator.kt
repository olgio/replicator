package ru.splite.replicator.atlas.id

/**
 * Генерация идентификаторов команд
 */
interface IdGenerator<S> {

    /**
     * Генерация нового идентификатора команды
     */
    fun generateNext(): Id<S>
}