package ru.splite.replicator.atlas.graph

class EmptyDependencyGraphStore<K> : DependencyGraphStore<K> {
    override suspend fun setDependenciesPerKey(key: K, dependencies: Set<K>) = Unit

    override suspend fun deleteDependenciesPerKey(key: K) = Unit

    override suspend fun getDependencies(): Sequence<Pair<K, Set<K>>> = emptySequence()

    override suspend fun setStatusPerKey(key: K, status: DependencyStatus) = Unit

    override suspend fun deleteStatusPerKey(key: K) = Unit

    override suspend fun getStatuses(): Sequence<Pair<K, DependencyStatus>> = emptySequence()
}