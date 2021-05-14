package ru.splite.replicator.demo

import kotlinx.coroutines.CoroutineName
import org.kodein.di.DI
import org.kodein.di.bind
import org.kodein.di.instance
import org.kodein.di.singleton
import ru.splite.replicator.atlas.AtlasCommandSubmitter
import ru.splite.replicator.atlas.AtlasMessage
import ru.splite.replicator.atlas.AtlasProtocolConfig
import ru.splite.replicator.atlas.AtlasProtocolController
import ru.splite.replicator.atlas.executor.CommandExecutor
import ru.splite.replicator.atlas.graph.*
import ru.splite.replicator.atlas.id.IdGenerator
import ru.splite.replicator.atlas.id.InMemoryIdGenerator
import ru.splite.replicator.atlas.protocol.AtlasProtocol
import ru.splite.replicator.atlas.protocol.BaseAtlasProtocol
import ru.splite.replicator.atlas.state.CommandStateStore
import ru.splite.replicator.atlas.state.InMemoryCommandStateStore
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
            CommandExecutor(instance(), instance(), instance())
        }

        bind<DependencyGraphStore<Dependency>>() with singleton { EmptyDependencyGraphStore() }

        bind<DependencyGraph<Dependency>>() with singleton { JGraphTDependencyGraph(instance()) }

        bind<MessageSender<AtlasMessage>>() with singleton {
            MessageSender(
                instance(),
                instance<AtlasProtocolConfig>().sendMessageTimeout
            )
        }

        bind<CommandStateStore>() with singleton {
            InMemoryCommandStateStore()
        }

        bind<AtlasProtocol>() with singleton {
            BaseAtlasProtocol(
                instance(),
                instance(),
                instance(),
                instance(),
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