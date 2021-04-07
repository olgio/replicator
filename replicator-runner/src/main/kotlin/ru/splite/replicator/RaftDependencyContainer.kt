package ru.splite.replicator

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import org.kodein.di.DI
import org.kodein.di.bind
import org.kodein.di.instance
import org.kodein.di.singleton
import ru.splite.replicator.log.InMemoryReplicatedLogStore
import ru.splite.replicator.log.ReplicatedLogStore
import ru.splite.replicator.raft.RaftCommandSubmitter
import ru.splite.replicator.raft.RaftProtocolConfig
import ru.splite.replicator.raft.RaftProtocolController
import ru.splite.replicator.raft.TermClockScheduler
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
                termClockPeriod = config.termClockPeriodRange,
                appendEntriesSendPeriod = config.appendEntriesSendPeriodRange
            )
        }

//        bind<MessageSender<AtlasMessage>>() with singleton {
//            MessageSender(
//                instance(),
//                instance<AtlasProtocolConfig>().sendMessageTimeout
//            )
//        }

        bind<RaftLocalNodeState>() with singleton { RaftLocalNodeState() }

        bind<TermClockScheduler>() with singleton {
            TermClockScheduler(instance(), instance())
        }

        bind<ReplicatedLogStore>() with singleton { InMemoryReplicatedLogStore() }

        bind<RaftProtocolController>() with singleton {
            RaftProtocolController(
                instance(),
                instance(),
                instance(),
                instance()
            )
        }

        bind<StateMachineCommandSubmitter<ByteArray, ByteArray>>() with singleton {
            val config = instance<RaftProtocolConfig>()
            val coroutineScope = instance<CoroutineScope>()
            instance<TermClockScheduler>().apply {
                launchTermClock(CoroutineName("term-clock"), coroutineScope, config.termClockPeriod)
            }
            RaftCommandSubmitter(instance(), instance(), instance()).apply {
                launchCommandApplier(CoroutineName("command-applier"), coroutineScope)
                launchAppendEntriesSender(
                    CoroutineName("entries-sender"),
                    coroutineScope,
                    config.appendEntriesSendPeriod
                )
            }
        }

    }
}