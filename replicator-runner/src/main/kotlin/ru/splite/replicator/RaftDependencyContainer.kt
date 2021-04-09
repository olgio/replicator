package ru.splite.replicator

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import org.kodein.di.DI
import org.kodein.di.bind
import org.kodein.di.instance
import org.kodein.di.singleton
import ru.splite.replicator.log.InMemoryReplicatedLogStore
import ru.splite.replicator.log.ReplicatedLogStore
import ru.splite.replicator.raft.*
import ru.splite.replicator.raft.state.RaftLocalNodeState
import ru.splite.replicator.statemachine.StateMachineCommandSubmitter

object RaftDependencyContainer {

    val module = DI.Module("raft") {
        bind<RaftProtocolConfig>() with singleton {
            val config = instance<RunnerConfig>()
            RaftProtocolConfig(
                address = config.nodeIdentifier,
                n = config.nodes.size,
                sendMessageTimeout = config.messageTimeout.toLong(),
                commandExecutorTimeout = config.commandExecutorTimeout.toLong(),
                termClockPeriod = config.termClockPeriodRange,
                appendEntriesSendPeriod = config.appendEntriesSendPeriodRange
            )
        }

        bind<RaftLocalNodeState>() with singleton { RaftLocalNodeState() }

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