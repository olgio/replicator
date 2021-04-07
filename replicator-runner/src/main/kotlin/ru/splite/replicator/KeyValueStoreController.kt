package ru.splite.replicator

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
import ru.splite.replicator.keyvalue.KeyValueCommand
import ru.splite.replicator.keyvalue.KeyValueReply
import ru.splite.replicator.statemachine.StateMachineCommandSubmitter
import ru.splite.replicator.transport.NodeIdentifier

class KeyValueStoreController(
    private val port: Int,
    private val nodeIdentifier: NodeIdentifier,
    private val stateMachineCommandSubmitter: StateMachineCommandSubmitter<ByteArray, ByteArray>
) {

    fun start() {
        embeddedServer(Netty, port = port) {

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
            }
        }.start(wait = false)
    }

    private suspend fun submitCommand(command: KeyValueCommand): KeyValueReply {
        val start = System.currentTimeMillis()
        val resultBytes = stateMachineCommandSubmitter.submit(KeyValueCommand.serialize(command))
        LOGGER.debug("Submitted command $command in ${System.currentTimeMillis() - start} ms")
        return KeyValueReply.deserializer(resultBytes)
    }

    companion object {
        val LOGGER: Logger = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}