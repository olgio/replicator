package ru.splite.replicator.statemachine

interface TransactionalStateMachine<T> : StateMachine<T> {

    fun prepare(command: T)

    fun rollback(command: T)
}