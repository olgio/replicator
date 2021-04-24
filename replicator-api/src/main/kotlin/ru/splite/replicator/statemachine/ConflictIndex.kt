package ru.splite.replicator.statemachine

/**
 * Индекс конфликтующих команд.
 * @param K тип ключа индекса
 * @param T тип команды конечного автомата
 */
interface ConflictIndex<K, T> {

    /**
     * Вставка команды [command] по ключу [key] в индекс.
     * @return множество ключей команд, конфликтующих с командой [command]
     */
    fun putAndGetConflicts(key: K, command: T): Set<K>
}