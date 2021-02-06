package ru.splite.replicator.statemachine

interface TransactionalStateMachine<T, R> : StateMachine<T, R> {

    fun prepare(command: T)

    fun rollback(command: T)
}