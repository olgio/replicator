package ru.splite.replicator.graph

data class KeysToExecute<K>(val executable: List<K>, val blockers: List<K>)