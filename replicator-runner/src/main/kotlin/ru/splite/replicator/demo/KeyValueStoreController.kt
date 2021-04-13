package ru.splite.replicator.demo

import com.google.common.base.Stopwatch
import io.ktor.application.*
import io.ktor.features.*
import io.ktor.http.*
import io.ktor.request.*
import io.ktor.response.*
import io.ktor.routing.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.splite.replicator.demo.keyvalue.KeyValueCommand
import ru.splite.replicator.demo.keyvalue.KeyValueReply
import ru.splite.replicator.metrics.Metrics
import ru.splite.replicator.metrics.Metrics.recordStopwatch
import ru.splite.replicator.statemachine.StateMachineCommandSubmitter
import ru.splite.replicator.transport.NodeIdentifier
import java.util.concurrent.TimeUnit


class KeyValueStoreController(
    private val port: Int,
    private val nodeIdentifier: NodeIdentifier,
    private val stateMachineCommandSubmitter: StateMachineCommandSubmitter<ByteArray, ByteArray>
) {

    fun start() {
        embeddedServer(Netty, port = port) {

//            install(MicrometerMetrics) {
//                registry = Metrics.appMicrometerRegistry
//                meterBinders = listOf(
//                    JvmMemoryMetrics(),
//                    JvmGcMetrics(),
//                    ProcessorMetrics()
//                )
//            }

            install(StatusPages) {
                exception<Throwable> { cause ->
                    call.respond(HttpStatusCode.InternalServerError, cause.toString())
                    LOGGER.error("Error while handling request", cause)
                }
            }

            install(DefaultHeaders) {
                header("Node-Identifier", nodeIdentifier.identifier)
            }

            routing {
                route("/kv") {
                    get("{key}") {
                        val key = call.parameters["key"] ?: error("Key must be specified")
                        val command = KeyValueCommand.GetValue(key)
                        LOGGER.debug("Received GET command $command")
                        val commandReply = submitCommand(command)
                        call.respondText(Json.encodeToString(commandReply), ContentType.Application.Json)
                    }

                    post("{key}") {
                        val key = call.parameters["key"] ?: error("Key must be specified")
                        val value = call.receiveText()
                        val command = KeyValueCommand.PutValue(key, value)
                        LOGGER.debug("Received PUT command $command")
                        val commandReply = submitCommand(command)
                        call.respondText(Json.encodeToString(commandReply), ContentType.Application.Json)
                    }
                }

                get("health") {
                    call.respondText("OK")
                }

//                get("metrics") {
//                    call.respond(Metrics.appMicrometerRegistry.scrape())
//                }
            }
        }.start(wait = false)
    }

    private suspend fun submitCommand(command: KeyValueCommand): KeyValueReply {
        val stopwatch = Stopwatch.createStarted()
        val responseResult = kotlin.runCatching {
            val resultBytes = stateMachineCommandSubmitter.submit(KeyValueCommand.serialize(command))
            KeyValueReply.deserializer(resultBytes)
        }
        stopwatch.stop()
        LOGGER.debug("Submitted command $command in ${stopwatch.elapsed(TimeUnit.MILLISECONDS)} ms")

        if (responseResult.isSuccess) {
            Metrics.registry.commandSubmitLatency.recordStopwatch(stopwatch)
        } else {
            Metrics.registry.commandSubmitErrorLatency.recordStopwatch(stopwatch)
        }
        return responseResult.getOrThrow()
    }

    companion object {
        val LOGGER: Logger = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}