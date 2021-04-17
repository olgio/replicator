package ru.splite.replicator.atlas.graph

data class KeysToExecute<K>(val executable: List<K>, val blockers: List<K>)