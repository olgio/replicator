package ru.splite.replicator.transport

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.test.runBlockingTest
import org.assertj.core.api.Assertions.assertThat
import ru.splite.replicator.demo.Command
import ru.splite.replicator.transport.*
import ru.splite.replicator.transport.sender.MessageSender
import java.util.*
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test

/**
 * Тесты отправки сообщений через вспомогательный [MessageSender]
 * Используется реализация транспорта [CoroutineChannelTransport]
 */
class MessageSenderTests {

    @Test
    fun sendOrNullSuccessTest(): Unit = runBlockingTest {
        val transport = buildTransport()

        val rootReceiver = transport.createReceiver { _, command ->
            command
        }
        val messageSender = MessageSender(rootReceiver)

        kotlin.run {
            var valueReceived = false
            val receiver1 = transport.createReceiver { _, command ->
                valueReceived = true
                command
            }

            val request = Command(1)
            val response = messageSender.sendOrNull(receiver1.address, request)
            assertThat(valueReceived).isTrue
            assertThat(response).isNotNull.isEqualTo(request)
        }
    }

    @Test
    fun sendOrNullExceptionTest(): Unit = runBlockingTest {
        val transport = buildTransport()

        val rootReceiver = transport.createReceiver { _, command ->
            command
        }
        val messageSender = MessageSender(rootReceiver)

        kotlin.run {
            val receiver1 = transport.createReceiver { _, command ->
                error("Expected")
            }

            val request = Command(1)
            val response = messageSender.sendOrNull(receiver1.address, request)
            assertThat(response).isNull()
        }
    }

    @Test
    fun sendOrThrowExceptionTest(): Unit = runBlockingTest {
        val transport = buildTransport()

        val rootReceiver = transport.createReceiver { _, command ->
            command
        }
        val messageSender = MessageSender(rootReceiver)

        kotlin.run {
            val receiver1 = transport.createReceiver { _, command ->
                error("Expected")
            }
            val request = Command(1)

            val response = kotlin.runCatching {
                messageSender.sendOrThrow(receiver1.address, request)
            }
            assertThat(response.exceptionOrNull()).isNotNull.hasStackTraceContaining("Expected")
        }
    }

    @Test
    fun sendToAllSuccessTest(): Unit = runBlockingTest {
        val transport = buildTransport()

        val rootReceiver = transport.createReceiver { _, command ->
            command
        }
        val messageSender = MessageSender(rootReceiver)

        kotlin.run {
            val receivedCount = AtomicInteger(0)
            val receiver1 = transport.createReceiver { _, command ->
                receivedCount.incrementAndGet()
                command
            }

            val receiver2 = transport.createReceiver { _, command ->
                receivedCount.incrementAndGet()
                command
            }
            val request = Command(1)

            val responses = messageSender.sendToAllOrThrow(listOf(receiver1.address, receiver2.address)) {
                request
            }
            assertThat(receivedCount.get()).isEqualTo(2)
            assertThat(responses).hasSize(2)
        }
    }

    @Test
    fun sendToAllAsFlowSuccessTest(): Unit = runBlockingTest {
        val transport = buildTransport()

        val rootReceiver = transport.createReceiver { _, command ->
            command
        }
        val messageSender = MessageSender(rootReceiver)

        val receivedCount = AtomicInteger(0)
        val responseReceivedCount = AtomicInteger(0)
        val receiver1 = transport.createReceiver { _, _ ->
            delay(800)
            Command(receivedCount.incrementAndGet().toLong())
        }

        val receiver2 = transport.createReceiver { _, _ ->
            delay(500)
            Command(receivedCount.incrementAndGet().toLong())
        }

        val request = Command(0)
        launch {
            messageSender.sendToAllAsFlow(listOf(receiver1.address, receiver2.address)) {
                request
            }.collect {
                assertThat(responseReceivedCount.incrementAndGet()).isEqualTo(receivedCount.get())
            }
        }
        assertThat(receivedCount.get()).isEqualTo(0)
        assertThat(responseReceivedCount.get()).isEqualTo(0)
        advanceTimeBy(500)

        assertThat(receivedCount.get()).isEqualTo(1)
        assertThat(responseReceivedCount.get()).isEqualTo(1)
        advanceTimeBy(500)

        assertThat(receivedCount.get()).isEqualTo(2)
        assertThat(responseReceivedCount.get()).isEqualTo(2)
    }

    @Test
    fun sendToAllAsFlowCancelTest(): Unit = runBlockingTest {
        val transport = buildTransport()

        val rootReceiver = transport.createReceiver { _, command ->
            command
        }
        val messageSender = MessageSender(rootReceiver)

        val receivedCount = AtomicInteger(0)
        val responseReceivedCount = AtomicInteger(0)
        val receiver1 = transport.createReceiver { _, _ ->
            delay(800)
            Command(receivedCount.incrementAndGet().toLong())
        }

        val receiver2 = transport.createReceiver { _, _ ->
            delay(500)
            Command(receivedCount.incrementAndGet().toLong())
        }

        val request = Command(0)
        val job = launch {
            messageSender.sendToAllAsFlow(listOf(receiver1.address, receiver2.address)) {
                request
            }.collect {
                assertThat(responseReceivedCount.incrementAndGet()).isEqualTo(receivedCount.get())
            }
        }

        assertThat(receivedCount.get()).isEqualTo(0)
        assertThat(responseReceivedCount.get()).isEqualTo(0)
        advanceTimeBy(500)

        assertThat(receivedCount.get()).isEqualTo(1)
        assertThat(responseReceivedCount.get()).isEqualTo(1)
        job.cancel()
        assertThat(job.isCompleted).isTrue
        advanceTimeBy(500)

        assertThat(receivedCount.get()).isEqualTo(2)
        assertThat(responseReceivedCount.get()).isEqualTo(1)
    }

    private fun CoroutineScope.buildTransport(): CoroutineChannelTransport {
        return CoroutineChannelTransport(this)
    }

    private fun Transport.createReceiver(action: suspend TypedActor<Command>.(NodeIdentifier, Command) -> Command): TypedActor<Command> {
        val nodeIdentifier = NodeIdentifier(UUID.randomUUID().toString())

        return object : TypedActor<Command>(nodeIdentifier, this@createReceiver, Command.serializer()) {
            override suspend fun receive(src: NodeIdentifier, payload: Command): Command {
                return action(src, payload)
            }
        }
    }
}