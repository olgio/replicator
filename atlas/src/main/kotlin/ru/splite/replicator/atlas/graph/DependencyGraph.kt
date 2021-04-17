package ru.splite.replicator.atlas.graph

interface DependencyGraph<K : Comparable<K>> {

    val numVertices: Int

    fun commit(key: K, dependencies: Set<K>)

    fun evaluateKeyToExecute(): KeysToExecute<K>
}