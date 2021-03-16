package ru.splite.replicator

import org.slf4j.LoggerFactory
import ru.splite.replicator.bus.NodeIdentifier
import ru.splite.replicator.transport.Transport
import ru.splite.replicator.transport.TypedActor

class AtlasProtocolController(
    transport: Transport,
    val protocol: AtlasProtocol
) : TypedActor<AtlasMessage>(protocol.address, transport, AtlasMessage.serializer()) {

    override suspend fun receive(src: NodeIdentifier, payload: AtlasMessage): AtlasMessage {
        val response = when (payload) {
            is AtlasMessage.MCollect -> protocol.handleCollect(src, payload)
            is AtlasMessage.MCommit -> protocol.handleCommit(payload)
            is AtlasMessage.MConsensus -> protocol.handleConsensus(payload)
            is AtlasMessage.MRecovery -> protocol.handleRecovery(payload)
            else -> error("Received unknown type of message: $payload")
        }
        LOGGER.debug("Received $src -> ${this.address}: $payload -> $response")
        return response
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}