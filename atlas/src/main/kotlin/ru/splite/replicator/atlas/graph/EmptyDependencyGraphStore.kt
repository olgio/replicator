package ru.splite.replicator.atlas.graph

class EmptyDependencyGraphStore<K> : DependencyGraphStore<K> {
    override fun setDependenciesPerKey(key: K, dependencies: Set<K>) = Unit

    override fun deleteDependenciesPerKey(key: K) = Unit

    override fun getDependencies(): Sequence<Pair<K, Set<K>>> = emptySequence()

    override fun setStatusPerKey(key: K, status: DependencyStatus) = Unit

    override fun deleteStatusPerKey(key: K) = Unit

    override fun getStatuses(): Sequence<Pair<K, DependencyStatus>> = emptySequence()
}