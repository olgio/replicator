package ru.splite.replicator.statemachine

interface StateMachine<T, R> {

    fun commit(command: T): R

    fun <K> newConflictIndex(): ConflictIndex<K, T>
}