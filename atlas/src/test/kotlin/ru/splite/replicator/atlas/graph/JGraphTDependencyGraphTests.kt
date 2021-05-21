package ru.splite.replicator.atlas.graph

import kotlinx.coroutines.test.runBlockingTest
import org.assertj.core.api.Assertions.assertThat
import ru.splite.replicator.atlas.id.Id
import ru.splite.replicator.transport.NodeIdentifier
import kotlin.test.Test

class JGraphTDependencyGraphTests {

    @Test
    fun reverseOrderCommitTest() = runBlockingTest {
        val dependencyGraph = JGraphTDependencyGraph<Dependency>()
        val dependency1 = createDependency(1, 1)
        val dependency2 = createDependency(1, 2)
        val dependency3 = createDependency(2, 1)
        val dependency4 = createDependency(3, 1)

        dependencyGraph.commit(dependency4, setOf(dependency2, dependency3))
        dependencyGraph.commit(dependency2, setOf(dependency1))
        dependencyGraph.evaluateKeyToExecute().let {
            assertThat(it.executable).isEmpty()
            assertThat(it.blockers).containsExactlyInAnyOrder(dependency1, dependency3)
        }

        dependencyGraph.commit(dependency1, emptySet())
        dependencyGraph.evaluateKeyToExecute().let {
            assertThat(it.executable).containsExactly(dependency1, dependency2)
            assertThat(it.blockers).containsExactlyInAnyOrder(dependency3)
        }

        dependencyGraph.commit(dependency3, emptySet())
        dependencyGraph.evaluateKeyToExecute().let {
            assertThat(it.executable).containsExactly(dependency3, dependency4)
            assertThat(it.blockers).isEmpty()
        }
    }

    @Test
    fun normalOrderCommitTest() = runBlockingTest {
        val dependencyGraph = JGraphTDependencyGraph<Dependency>()
        val dependency1 = createDependency(1, 1)
        val dependency2 = createDependency(1, 2)
        val dependency3 = createDependency(2, 1)
        val dependency4 = createDependency(3, 1)

        dependencyGraph.commit(dependency2, setOf(dependency1))
        dependencyGraph.commit(dependency1, emptySet())

        dependencyGraph.evaluateKeyToExecute().let {
            assertThat(it.executable).containsExactly(dependency1, dependency2)
            assertThat(it.blockers).isEmpty()
        }

        dependencyGraph.commit(dependency3, emptySet())
        dependencyGraph.commit(dependency4, setOf(dependency2, dependency3))
        dependencyGraph.evaluateKeyToExecute().let {
            assertThat(it.executable).containsExactly(dependency3, dependency4)
            assertThat(it.blockers).isEmpty()
        }
    }

    @Test
    fun strongConnectedComponentCommitTest() = runBlockingTest {
        val dependencyGraph = JGraphTDependencyGraph<Dependency>()
        val dependency1 = createDependency(1, 1)
        val dependency2 = createDependency(1, 2)
        val dependency3 = createDependency(2, 1)

        dependencyGraph.commit(dependency2, setOf(dependency1, dependency3))
        dependencyGraph.evaluateKeyToExecute().let {
            assertThat(it.executable).isEmpty()
            assertThat(it.blockers).containsExactlyInAnyOrder(dependency1, dependency3)
        }

        dependencyGraph.commit(dependency3, setOf(dependency1, dependency2))
        dependencyGraph.commit(dependency1, setOf(dependency2))
        dependencyGraph.evaluateKeyToExecute().let {
            assertThat(it.executable).containsExactly(dependency1, dependency2, dependency3)
            assertThat(it.blockers).isEmpty()
        }
    }

    private fun createDependency(nodeId: Int, recordId: Int): Dependency {
        return Dependency(Id(NodeIdentifier.fromInt(nodeId), recordId.toLong()))
    }
}