package ru.splite.replicator.statemachine

interface ConflictOrderedStateMachine<T, R> : StateMachine<T, R> {

    fun <K> newConflictIndex(): ConflictIndex<K, T>
}