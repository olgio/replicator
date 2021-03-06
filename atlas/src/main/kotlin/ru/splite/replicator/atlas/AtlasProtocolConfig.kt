package ru.splite.replicator.atlas

import ru.splite.replicator.transport.NodeIdentifier

data class AtlasProtocolConfig(
    val address: NodeIdentifier,
    val processId: Long,
    val n: Int,
    val f: Int,
    val sendMessageTimeout: Long = 1000,
    val commandExecutorTimeout: Long = 3000,
    val commandRecoveryDelay: LongRange = 2000L..4000L,
    val enableRecovery: Boolean = true
) {

    init {
        assert(n > 2) {
            "n must be more than 2 but received $n"
        }

        val maxF = (n - 1) / 2
        assert(f in 1..maxF) {
            "f must be in range [1, $maxF] but received $f"
        }
    }

    val fastQuorumSize: Int = n / 2 + f

    val slowQuorumSize: Int = f + 1

    val recoveryQuorumSize: Int = n - f
}