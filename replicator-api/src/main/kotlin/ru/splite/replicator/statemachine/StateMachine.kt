package ru.splite.replicator.statemachine

interface StateMachine<T, R> {

    fun apply(command: T): R
}