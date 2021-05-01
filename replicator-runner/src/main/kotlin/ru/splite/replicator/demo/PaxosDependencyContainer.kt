package ru.splite.replicator.demo

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import org.kodein.di.DI
import org.kodein.di.bind
import org.kodein.di.instance
import org.kodein.di.singleton
import ru.splite.replicator.log.InMemoryReplicatedLogStore
import ru.splite.replicator.log.ReplicatedLogStore
import ru.splite.replicator.paxos.PaxosProtocolController
import ru.splite.replicator.paxos.protocol.BasePaxosProtocol
import ru.splite.replicator.paxos.protocol.PaxosProtocol
import ru.splite.replicator.raft.JobLauncher
import ru.splite.replicator.raft.RaftCommandSubmitter
import ru.splite.replicator.raft.RaftProtocolConfig
import ru.splite.replicator.raft.state.InMemoryNodeStateStore
import ru.splite.replicator.raft.state.NodeStateStore
import ru.splite.replicator.statemachine.StateMachineCommandSubmitter

object PaxosDependencyContainer {

    val module = DI.Module("paxos") {
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

        bind<PaxosProtocol>() with singleton {
            BasePaxosProtocol(instance(), instance(), instance())
        }

        bind<PaxosProtocolController>() with singleton {
            PaxosProtocolController(instance(), instance(), instance())
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