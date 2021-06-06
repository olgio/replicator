package ru.splite.replicator.demo

import kotlinx.coroutines.Dispatchers
import org.kodein.di.DI
import org.kodein.di.bind
import org.kodein.di.instance
import org.kodein.di.singleton
import ru.splite.replicator.atlas.AtlasProtocolConfig
import ru.splite.replicator.atlas.graph.Dependency
import ru.splite.replicator.atlas.graph.DependencyGraphStore
import ru.splite.replicator.atlas.id.IdGenerator
import ru.splite.replicator.atlas.rocksdb.RocksDbCommandStateStore
import ru.splite.replicator.atlas.rocksdb.RocksDbDependencyGraphStore
import ru.splite.replicator.atlas.rocksdb.RocksDbIdGenerator
import ru.splite.replicator.atlas.state.CommandStateStore
import ru.splite.replicator.log.ReplicatedLogStore
import ru.splite.replicator.raft.rocksdb.RocksDbNodeStateStore
import ru.splite.replicator.raft.rocksdb.RocksDbReplicatedLogStore
import ru.splite.replicator.raft.state.NodeStateStore
import ru.splite.replicator.rocksdb.RocksDbKeyValueConflictIndex
import ru.splite.replicator.rocksdb.RocksDbKeyValueStateMachine
import ru.splite.replicator.rocksdb.RocksDbStore
import ru.splite.replicator.statemachine.ConflictIndex
import ru.splite.replicator.statemachine.ConflictOrderedStateMachine
import ru.splite.replicator.transport.NodeIdentifier
import java.io.File

object RocksDbDependencyContainer {

    val module = DI.Module("rocksdb", allowSilentOverride = true) {

        bind<RocksDbStore>() with singleton {
            val file = File(instance<RunnerConfig>().rocksDbFile!!)
            RocksDbStore(
                file,
                RocksDbNodeStateStore.COLUMN_FAMILY_NAMES
                        + RocksDbReplicatedLogStore.COLUMN_FAMILY_NAMES
                        + RocksDbKeyValueStateMachine.COLUMN_FAMILY_NAMES
                        + RocksDbKeyValueConflictIndex.COLUMN_FAMILY_NAMES
                        + RocksDbCommandStateStore.COLUMN_FAMILY_NAMES
                        + RocksDbDependencyGraphStore.COLUMN_FAMILY_NAMES
                        + RocksDbIdGenerator.COLUMN_FAMILY_NAMES,
                Dispatchers.IO
            )
        }

        bind<NodeStateStore>() with singleton {
            RocksDbNodeStateStore(instance())
        }

        bind<ReplicatedLogStore>() with singleton {
            RocksDbReplicatedLogStore(instance())
        }

        bind<IdGenerator<NodeIdentifier>>() with singleton {
            RocksDbIdGenerator(instance<AtlasProtocolConfig>().address, instance())
        }

        bind<DependencyGraphStore<Dependency>>() with singleton {
            RocksDbDependencyGraphStore(instance())
        }

        bind<CommandStateStore>() with singleton {
            RocksDbCommandStateStore(instance())
        }

        bind<ConflictOrderedStateMachine<ByteArray, ByteArray>>() with singleton {
            RocksDbKeyValueStateMachine(instance())
        }

        bind<ConflictIndex<Dependency, ByteArray>>() with singleton {
            RocksDbKeyValueConflictIndex(instance())
        }

    }
}