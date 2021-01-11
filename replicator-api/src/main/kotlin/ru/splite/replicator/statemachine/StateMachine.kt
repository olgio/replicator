package ru.splite.replicator.statemachine

interface StateMachine<T> {

    fun commit(command: T)
}