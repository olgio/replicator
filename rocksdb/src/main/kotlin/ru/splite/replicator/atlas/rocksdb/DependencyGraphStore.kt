package ru.splite.replicator.atlas.rocksdb

interface DependencyGraphStore<K> {
    fun setDependenciesPerKey(key: K, dependencies: Set<K>)

    fun deleteDependenciesPerKey(key: K)

    fun getDependencies(): Sequence<Pair<K, Set<K>>>

    fun setStatusPerKey(key: K, status: DependencyStatus)

    fun deleteStatusPerKey(key: K)

    fun getStatuses(): Sequence<Pair<K, DependencyStatus>>
}