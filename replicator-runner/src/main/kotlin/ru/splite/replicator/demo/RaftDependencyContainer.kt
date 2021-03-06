package ru.splite.replicator.demo

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import org.kodein.di.DI
import org.kodein.di.bind
import org.kodein.di.instance
import org.kodein.di.singleton
import ru.splite.replicator.log.InMemoryReplicatedLogStore
import ru.splite.replicator.log.ReplicatedLogStore
import ru.splite.replicator.raft.JobLauncher
import ru.splite.replicator.raft.RaftCommandSubmitter
import ru.splite.replicator.raft.RaftProtocolConfig
import ru.splite.replicator.raft.RaftProtocolController
import ru.splite.replicator.raft.protocol.BaseRaftProtocol
import ru.splite.replicator.raft.protocol.RaftProtocol
import ru.splite.replicator.raft.state.InMemoryNodeStateStore
import ru.splite.replicator.raft.state.NodeStateStore
import ru.splite.replicator.statemachine.StateMachineCommandSubmitter

object RaftDependencyContainer {

    val module = DI.Module("raft") {
        bind<RaftProtocolConfig>() with singleton {
            val config = instance<RunnerConfig>()
            RaftProtocolConfig(
                address = config.nodeIdentifier,
                processId = config.nodeIdentifier.identifier.toLong(),
                n = config.nodes.size,
                sendMessageTimeout = config.messageTimeout.toLong(),
                commandExecutorTimeout = config.commandExecutorTimeout.toLong(),
                termClockPeriod = config.termClockPeriodRange,
                appendEntriesSendPeriod = config.appendEntriesSendPeriodRange
            )
        }

        bind<NodeStateStore>() with singleton { InMemoryNodeStateStore() }

        bind<JobLauncher>() with singleton {
            JobLauncher(instance(), instance())
        }

        bind<ReplicatedLogStore>() with singleton { InMemoryReplicatedLogStore() }

        bind<RaftProtocol>() with singleton {
            BaseRaftProtocol(instance(), instance(), instance())
        }

        bind<RaftProtocolController>() with singleton {
            RaftProtocolController(instance(), instance(), instance())
        }

        bind<StateMachineCommandSubmitter<ByteArray, ByteArray>>() with singleton {
            val config = instance<RaftProtocolConfig>()
            val coroutineScope = instance<CoroutineScope>()
            instance<JobLauncher>().apply {
                launchTermClock(CoroutineName("term-clock"), coroutineScope, config.termClockPeriod)
                launchAppendEntriesSender(
                    CoroutineName("entries-sender"),
                    coroutineScope,
                    config.appendEntriesSendPeriod
                )
            }
            RaftCommandSubmitter(instance(), instance()).apply {
                launchCommandApplier(CoroutineName("command-applier"), coroutineScope)
            }
        }

    }
}