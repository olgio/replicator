package ru.splite.replicator.statemachine

interface StateMachine<T, R> {

    fun commit(command: T): R
}