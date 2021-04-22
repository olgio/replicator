package ru.splite.replicator.statemachine

/**
 * Интерфейс конечного автомата с поддержой упорядочивания конфликтующих команд.
 * @param T тип команды конечного автомата
 * @param R результат применения команды к конечному автомату
 */
interface ConflictOrderedStateMachine<T, R> : StateMachine<T, R> {

    /**
     * Создание индекса конфликтующих команд.
     * @param K тип ключа индекса
     * @return индекс конфликтующих команд
     */
    fun <K> newConflictIndex(): ConflictIndex<K, T>
}