package ru.splite.replicator.atlas.rocksdb

import kotlinx.serialization.Serializable
import ru.splite.replicator.atlas.graph.Dependency
import ru.splite.replicator.atlas.graph.DependencyGraphStore
import ru.splite.replicator.atlas.graph.DependencyStatus
import ru.splite.replicator.rocksdb.RocksDbStore

class RocksDbDependencyGraphStore(db: RocksDbStore) : DependencyGraphStore<Dependency> {

    @Serializable
    private data class SetWrapper(val dependencies: Set<Dependency>)

    private val dependencyGraphStore = db.createColumnFamilyStore(DEPENDENCY_GRAPH_COLUMN_FAMILY_NAME)

    private val dependencyStatusStore = db.createColumnFamilyStore(DEPENDENCY_STATUS_COLUMN_FAMILY_NAME)

    override suspend fun setDependenciesPerKey(key: Dependency, dependencies: Set<Dependency>) {
        dependencyGraphStore.putAsType(
            key,
            SetWrapper(dependencies),
            Dependency.serializer(),
            SetWrapper.serializer()
        )
    }

    override suspend fun deleteDependenciesPerKey(key: Dependency) {
        dependencyGraphStore.delete(key, Dependency.serializer())
    }

    override suspend fun getDependencies(): Sequence<Pair<Dependency, Set<Dependency>>> {
        return dependencyGraphStore.getAll(Dependency.serializer(), SetWrapper.serializer()).map {
            it.key to it.value.dependencies
        }
    }

    override suspend fun setStatusPerKey(key: Dependency, status: DependencyStatus) {
        dependencyStatusStore.putAsType(
            key,
            status,
            Dependency.serializer(),
            DependencyStatus.serializer()
        )
    }

    override suspend fun deleteStatusPerKey(key: Dependency) {
        dependencyStatusStore.delete(key, Dependency.serializer())
    }

    override suspend fun getStatuses(): Sequence<Pair<Dependency, DependencyStatus>> {
        return dependencyStatusStore.getAll(Dependency.serializer(), DependencyStatus.serializer()).map {
            it.key to it.value
        }
    }

    companion object {
        private const val DEPENDENCY_GRAPH_COLUMN_FAMILY_NAME = "DEPENDENCY_GRAPH"
        private const val DEPENDENCY_STATUS_COLUMN_FAMILY_NAME = "DEPENDENCY_STATUS"

        val COLUMN_FAMILY_NAMES = listOf(
            DEPENDENCY_GRAPH_COLUMN_FAMILY_NAME,
            DEPENDENCY_STATUS_COLUMN_FAMILY_NAME
        )
    }
}