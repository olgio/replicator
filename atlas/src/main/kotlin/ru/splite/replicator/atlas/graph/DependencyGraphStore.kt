package ru.splite.replicator.atlas.graph

interface DependencyGraphStore<K> {

    suspend fun setDependenciesPerKey(key: K, dependencies: Set<K>)

    suspend fun deleteDependenciesPerKey(key: K)

    suspend fun getDependencies(): Sequence<Pair<K, Set<K>>>

    suspend fun setStatusPerKey(key: K, status: DependencyStatus)

    suspend fun deleteStatusPerKey(key: K)

    suspend fun getStatuses(): Sequence<Pair<K, DependencyStatus>>
}