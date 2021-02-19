package ru.splite.replicator.statemachine

interface ConflictIndex<K, T> {

    fun putAndGetConflicts(key: K, command: T): Set<K>

    fun putAndGetConflicts(key: K, command: T, dependencies: Set<K>): Set<K>
}