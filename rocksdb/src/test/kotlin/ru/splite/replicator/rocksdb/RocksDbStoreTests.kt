package ru.splite.replicator.rocksdb

import kotlinx.coroutines.test.runBlockingTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.TestInstance
import ru.splite.replicator.atlas.graph.Dependency
import ru.splite.replicator.atlas.graph.DependencyStatus
import ru.splite.replicator.atlas.id.Id
import ru.splite.replicator.atlas.rocksdb.RocksDbCommandStateStore
import ru.splite.replicator.atlas.rocksdb.RocksDbDependencyGraphStore
import ru.splite.replicator.atlas.rocksdb.RocksDbIdGenerator
import ru.splite.replicator.atlas.state.CommandState
import ru.splite.replicator.demo.keyvalue.KeyValueCommand
import ru.splite.replicator.demo.keyvalue.KeyValueReply
import ru.splite.replicator.log.LogEntry
import ru.splite.replicator.raft.rocksdb.RocksDbNodeStateStore
import ru.splite.replicator.raft.rocksdb.RocksDbReplicatedLogStore
import ru.splite.replicator.raft.state.ExternalNodeState
import ru.splite.replicator.transport.NodeIdentifier
import java.io.File
import java.util.*
import kotlin.random.Random
import kotlin.test.Test

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RocksDbStoreTests {

    private val filename = "./testrocksdb"

    private val db: RocksDbStore = kotlin.run {
        val file = File(filename)
        file.deleteRecursively()
        RocksDbStore(
            file,
            RocksDbNodeStateStore.COLUMN_FAMILY_NAMES
                    + RocksDbReplicatedLogStore.COLUMN_FAMILY_NAMES
                    + RocksDbKeyValueStateMachine.COLUMN_FAMILY_NAMES
                    + RocksDbKeyValueConflictIndex.COLUMN_FAMILY_NAMES
                    + RocksDbCommandStateStore.COLUMN_FAMILY_NAMES
                    + RocksDbDependencyGraphStore.COLUMN_FAMILY_NAMES
                    + RocksDbIdGenerator.COLUMN_FAMILY_NAMES
        )
    }

    @Test
    fun localNodeStateTest() = runBlockingTest {
        val newTerm = Random.nextLong()
        val leaderIdentifier = NodeIdentifier("node-1")
        kotlin.run {
            val nodeStateStore = RocksDbNodeStateStore(db)
            nodeStateStore.setState(
                nodeStateStore.getState().copy(
                    currentTerm = newTerm, leaderIdentifier = leaderIdentifier
                )
            )
            val state = nodeStateStore.getState()
            assertThat(state.currentTerm).isEqualTo(newTerm)
            assertThat(state.leaderIdentifier).isEqualTo(leaderIdentifier)
        }

        kotlin.run {
            val nodeStateStore = RocksDbNodeStateStore(db)
            val state = nodeStateStore.getState()
            assertThat(state.currentTerm).isEqualTo(newTerm)
            assertThat(state.leaderIdentifier).isEqualTo(leaderIdentifier)
        }
    }

    @Test
    fun externalNodeStatesTest() = runBlockingTest {
        val nodeIdentifier = NodeIdentifier("node-1")
        val externalNodeState = ExternalNodeState(2L, 3L)
        kotlin.run {
            val nodeStateStore = RocksDbNodeStateStore(db)
            nodeStateStore.setExternalNodeState(
                nodeIdentifier,
                externalNodeState
            )
            assertThat(nodeStateStore.getExternalNodeState(nodeIdentifier))
                .isEqualTo(externalNodeState)
        }

        kotlin.run {
            val nodeStateStore = RocksDbNodeStateStore(db)
            assertThat(nodeStateStore.getExternalNodeState(nodeIdentifier))
                .isEqualTo(externalNodeState)
        }
    }

    @Test
    fun commandStateTest() = runBlockingTest {
        val nodeIdentifier = NodeIdentifier("node-1")
        val id = Id(nodeIdentifier, 0L)
        val commandState = CommandState(ballot = 2L)
        kotlin.run {
            val commandStateStore = RocksDbCommandStateStore(db)
            commandStateStore.setCommandState(
                id,
                commandState
            )
            assertThat(commandStateStore.getCommandState(id))
                .isEqualTo(commandState)
        }

        kotlin.run {
            val commandStateStore = RocksDbCommandStateStore(db)
            assertThat(commandStateStore.getCommandState(id))
                .isEqualTo(commandState)
        }
    }

    @Test
    fun dependencyGraphTest() = runBlockingTest {
        val nodeIdentifier = NodeIdentifier("node-1")
        val dependency1 = Dependency(Id(nodeIdentifier, 0L))
        val dependency2 = Dependency(Id(nodeIdentifier, 1L))

        kotlin.run {
            val dependencyGraphStore = RocksDbDependencyGraphStore(db)
            assertThat(dependencyGraphStore.getDependencies().toList())
                .isEmpty()

            dependencyGraphStore.setDependenciesPerKey(dependency1, setOf(dependency2))
            assertThat(dependencyGraphStore.getDependencies().toList())
                .containsExactlyInAnyOrder(dependency1 to setOf(dependency2))
        }

        kotlin.run {
            val dependencyGraphStore = RocksDbDependencyGraphStore(db)
            assertThat(dependencyGraphStore.getDependencies().toList())
                .containsExactlyInAnyOrder(dependency1 to setOf(dependency2))
            dependencyGraphStore.deleteDependenciesPerKey(dependency1)
            assertThat(dependencyGraphStore.getDependencies().toList())
                .isEmpty()
        }
    }

    @Test
    fun dependencyStatusTest() = runBlockingTest {
        val nodeIdentifier = NodeIdentifier("node-1")
        val dependency = Dependency(Id(nodeIdentifier, 0L))

        kotlin.run {
            val dependencyGraphStore = RocksDbDependencyGraphStore(db)
            assertThat(dependencyGraphStore.getStatuses().toList())
                .isEmpty()
            dependencyGraphStore.setStatusPerKey(dependency, DependencyStatus.COMMITTED)
            assertThat(dependencyGraphStore.getStatuses().toList())
                .containsExactlyInAnyOrder(dependency to DependencyStatus.COMMITTED)
        }

        kotlin.run {
            val dependencyGraphStore = RocksDbDependencyGraphStore(db)
            assertThat(dependencyGraphStore.getStatuses().toList())
                .containsExactlyInAnyOrder(dependency to DependencyStatus.COMMITTED)
            dependencyGraphStore.deleteStatusPerKey(dependency)
            assertThat(dependencyGraphStore.getStatuses().toList())
                .isEmpty()
        }
    }

    @Test
    fun idGeneratorTest() = runBlockingTest {
        val nodeIdentifier = NodeIdentifier("node-1")
        var prevId = -1L
        repeat(10) {
            val idGeneratorStore = RocksDbIdGenerator(nodeIdentifier, db, 3)
            val newId = idGeneratorStore.generateNext().id
            assertThat(newId).isGreaterThan(prevId)
            prevId = newId
        }
    }

    @Test
    fun replicatedLogTest() = runBlockingTest {

        val logEntry = LogEntry(0L, ByteArray(0))

        kotlin.run {
            val replicatedLog = RocksDbReplicatedLogStore(db)

            assertThat(replicatedLog.prune(0L)).isNull()

            assertThat(replicatedLog.lastCommitIndex()).isNull()
            assertThat(replicatedLog.lastLogIndex()).isNull()
            assertThat(replicatedLog.getLogEntryByIndex(0L)).isNull()

            assertThat(replicatedLog.appendLogEntry(logEntry)).isEqualTo(0L)
            assertThat(replicatedLog.lastCommitIndex()).isNull()
            assertThat(replicatedLog.lastLogIndex()).isEqualTo(0L)
            assertThat(replicatedLog.getLogEntryByIndex(0L)).isEqualTo(logEntry)

            assertThat(replicatedLog.commit(0L)).isEqualTo(0L)
            assertThat(replicatedLog.lastCommitIndex()).isEqualTo(0L)
        }

        kotlin.run {
            val replicatedLog = RocksDbReplicatedLogStore(db)

            assertThat(replicatedLog.lastCommitIndex()).isEqualTo(0L)
            assertThat(replicatedLog.lastLogIndex()).isEqualTo(0L)
            assertThat(replicatedLog.getLogEntryByIndex(0L)).isEqualTo(logEntry)

            assertThat(kotlin.runCatching {
                replicatedLog.prune(0L)
            }.exceptionOrNull())
                .isNotNull
                .hasStackTraceContaining("CommittedLogEntryOverrideException")

            assertThat(kotlin.runCatching {
                replicatedLog.setLogEntry(0L, logEntry)
            }.exceptionOrNull())
                .isNotNull
                .hasStackTraceContaining("CommittedLogEntryOverrideException")
        }
    }

    @Test
    fun keyValueTest() = runBlockingTest {
        val key = UUID.randomUUID().toString()
        kotlin.run {
            val keyValueStore = RocksDbKeyValueStateMachine(db)

            assertThat(
                KeyValueReply.deserializer(
                    keyValueStore.apply(
                        KeyValueCommand.newGetCommand(key)
                    )
                ).isEmpty
            ).isTrue

            assertThat(
                KeyValueReply.deserializer(
                    keyValueStore.apply(
                        KeyValueCommand.newPutCommand(key, "value1")
                    )
                ).isEmpty
            ).isFalse
        }

        kotlin.run {
            val keyValueStore = RocksDbKeyValueStateMachine(db)
            assertThat(
                KeyValueReply.deserializer(
                    keyValueStore.apply(
                        KeyValueCommand.newGetCommand(key)
                    )
                ).isEmpty
            ).isFalse
        }
    }

    @Test
    fun conflictIndexTest() = runBlockingTest {
        val key = "key"
        val nodeIdentifier = NodeIdentifier("node-1")
        val dependency1 = Dependency(Id(nodeIdentifier, 0L))
        val dependency2 = Dependency(Id(nodeIdentifier, 1L))
        val dependency3 = Dependency(Id(nodeIdentifier, 2L))
        val dependency4 = Dependency(Id(nodeIdentifier, 3L))

        kotlin.run {
            val conflictIndexStore = RocksDbKeyValueConflictIndex(db)

            assertThat(
                conflictIndexStore.putAndGetConflicts(
                    dependency1,
                    KeyValueCommand.newGetCommand(key)
                )
            ).isEmpty()

            assertThat(
                conflictIndexStore.putAndGetConflicts(
                    dependency2,
                    KeyValueCommand.newPutCommand(key, "value")
                )
            ).containsExactlyInAnyOrder(dependency1)
        }

        kotlin.run {
            val conflictIndexStore = RocksDbKeyValueConflictIndex(db)

            assertThat(
                conflictIndexStore.putAndGetConflicts(
                    dependency3,
                    KeyValueCommand.newPutCommand(key, "value")
                )
            ).containsExactlyInAnyOrder(dependency1, dependency2)

            assertThat(
                conflictIndexStore.putAndGetConflicts(
                    dependency4,
                    KeyValueCommand.newGetCommand(key)
                )
            ).containsExactlyInAnyOrder(dependency3)
        }
    }
}