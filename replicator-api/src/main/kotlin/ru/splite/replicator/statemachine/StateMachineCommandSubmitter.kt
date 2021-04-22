package ru.splite.replicator.statemachine

/**
 * Интерфейс отправки команд на репликацию и применение к конечному автомату.
 * Реализации предоставляются библиотекой.
 * Реализация должна быть потокобезопасной.
 * @param T тип команды конечного автомата
 * @param R результат применения команды к конечному автомату
 * @see StateMachine интерфейс конечного автомата
 */
interface StateMachineCommandSubmitter<T, R> {

    /**
     * Репликация команды [command].
     * Возвращает результат после подтверждения команды протоколом репликации
     * и ее применения к конечному автомату [StateMachine].
     * @return результат применение команды к конечному автомату
     */
    suspend fun submit(command: T): R
}