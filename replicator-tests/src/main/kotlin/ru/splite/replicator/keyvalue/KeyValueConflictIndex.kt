package ru.splite.replicator.keyvalue

import ru.splite.replicator.statemachine.ConflictIndex
import java.util.concurrent.ConcurrentHashMap

internal class KeyValueConflictIndex<K> : ConflictIndex<K, ByteArray> {

    private val conflictIndex = ConcurrentHashMap<String, MutableSet<K>>()

    override fun putAndGetConflicts(key: K, bytes: ByteArray): Set<K> {
        val storeKey = when (val command = KeyValueCommand.deserializer(bytes)) {
            is KeyValueCommand.GetValue -> command.key
            is KeyValueCommand.PutValue -> command.key
        }

        val keys = conflictIndex.getOrPut(storeKey) {
            mutableSetOf()
        }
        val keysView = keys.toSet()
        keys.add(key)
        return keysView
    }

    override fun putAndGetConflicts(key: K, command: ByteArray, dependencies: Set<K>): Set<K> {
        return putAndGetConflicts(key, command).plus(dependencies)
    }


}