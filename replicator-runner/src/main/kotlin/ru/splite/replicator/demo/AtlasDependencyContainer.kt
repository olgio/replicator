package ru.splite.replicator.demo

import kotlinx.coroutines.CoroutineName
import org.kodein.di.DI
import org.kodein.di.bind
import org.kodein.di.instance
import org.kodein.di.singleton
import ru.splite.replicator.*
import ru.splite.replicator.executor.CommandExecutor
import ru.splite.replicator.graph.Dependency
import ru.splite.replicator.graph.DependencyGraph
import ru.splite.replicator.graph.JGraphTDependencyGraph
import ru.splite.replicator.id.IdGenerator
import ru.splite.replicator.id.InMemoryIdGenerator
import ru.splite.replicator.statemachine.ConflictOrderedStateMachine
import ru.splite.replicator.statemachine.StateMachineCommandSubmitter
import ru.splite.replicator.transport.NodeIdentifier
import ru.splite.replicator.transport.TypedActor
import ru.splite.replicator.transport.sender.MessageSender

object AtlasDependencyContainer {

    val module = DI.Module("atlas") {
        bind<AtlasProtocolConfig>() with singleton {
            val config = instance<RunnerConfig>()
            AtlasProtocolConfig(
                address = config.nodeIdentifier,
                processId = config.nodeIdentifier.identifier.toLong(),
                n = config.nodes.size,
                f = config.f,
                sendMessageTimeout = config.messageTimeout.toLong(),
                commandExecutorTimeout = config.commandExecutorTimeout.toLong()
            )
        }

        bind<IdGenerator<NodeIdentifier>>() with singleton {
            InMemoryIdGenerator(instance<AtlasProtocolConfig>().address)
        }

        bind<CommandExecutor>() with singleton {
            CommandExecutor(instance(), instance())
        }

        bind<DependencyGraph<Dependency>>() with singleton { JGraphTDependencyGraph() }

        bind<MessageSender<AtlasMessage>>() with singleton {
            MessageSender(
                instance(),
                instance<AtlasProtocolConfig>().sendMessageTimeout
            )
        }

        bind<AtlasProtocol>() with singleton {
            BaseAtlasProtocol(
                instance(),
                instance(),
                instance<ConflictOrderedStateMachine<ByteArray, ByteArray>>().newConflictIndex(),
                instance()
            )
        }

        bind<TypedActor<AtlasMessage>>() with singleton { AtlasProtocolController(instance(), instance()) }

        bind<StateMachineCommandSubmitter<ByteArray, ByteArray>>() with singleton {
            instance<CommandExecutor>().apply {
                launchCommandExecutor(CoroutineName("command-executor"), instance())
            }
            AtlasCommandSubmitter(
                instance(),
                instance(),
                instance(),
                instance()
            ).apply {
                launchCommandRecoveryLoop(
                    CoroutineName("command-recovery"),
                    instance(),
                    instance()
                )
            }
        }

    }
}