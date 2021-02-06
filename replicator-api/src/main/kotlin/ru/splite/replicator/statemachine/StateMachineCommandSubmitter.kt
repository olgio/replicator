package ru.splite.replicator.statemachine

interface StateMachineCommandSubmitter<T, R> {

    suspend fun submit(command: T): R
}