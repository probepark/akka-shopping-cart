package shopping.cart

import akka.actor.CoordinatedShutdown
import akka.actor.testkit.typed.javadsl.ActorTestKit
import akka.actor.typed.ActorSystem
import akka.cluster.Member
import akka.cluster.MemberStatus
import akka.cluster.typed.Cluster
import akka.grpc.GrpcClientSettings
import akka.testkit.SocketUtil
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.junit.AfterClass
import org.junit.Assert
import org.junit.BeforeClass
import org.junit.Test
import org.springframework.orm.jpa.JpaTransactionManager
import scala.jdk.CollectionConverters
import shopping.cart.CreateTableTestUtils.createTables
import shopping.cart.Main.init
import shopping.cart.Main.orderServiceClient
import shopping.cart.proto.AddItemRequest
import shopping.cart.proto.ShoppingCartService
import shopping.cart.proto.ShoppingCartServiceClient
import shopping.cart.repository.SpringIntegration
import java.net.InetSocketAddress
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.function.Consumer
import java.util.function.Supplier
import java.util.stream.Collectors

class IntegrationTest {

    companion object {
//        private val logger = LoggerFactory.getLogger(IntegrationTest::class.java)

        private var testNode1: TestNodeFixture? = null
        private var testNode2: TestNodeFixture? = null
        private var testNode3: TestNodeFixture? = null
        private var systems: List<ActorSystem<*>>? = null
        private val requestTimeout = Duration.ofSeconds(10)

        private fun sharedConfig(): Config {
            return ConfigFactory.load("integration-test.conf")
        }

        private fun nodeConfig(grpcPort: Int, managementPorts: List<Int>, managementPortIndex: Int): Config {
            return ConfigFactory.parseString(
                """shopping-cart-service.grpc.port = $grpcPort
akka.management.http.port = ${managementPorts[managementPortIndex]}
akka.discovery.config.services.shopping-cart-service.endpoints = [
  { host = "127.0.0.1", port = ${managementPorts[0]}},
  { host = "127.0.0.1", port = ${managementPorts[1]}},
  { host = "127.0.0.1", port = ${managementPorts[2]}},
]"""
            )
        }

        @BeforeClass
        @JvmStatic
        fun setup() {
            val inetSocketAddresses = CollectionConverters.SeqHasAsJava(
                SocketUtil.temporaryServerAddresses(6, "127.0.0.1", false)
            )
                .asJava()
            val grpcPorts =
                inetSocketAddresses.subList(0, 3).stream()
                    .map { obj: InetSocketAddress -> obj.port }
                    .collect(Collectors.toList())
            val managementPorts = inetSocketAddresses.subList(3, 6).stream()
                .map { obj: InetSocketAddress -> obj.port }
                .collect(Collectors.toList())
            testNode1 = TestNodeFixture(grpcPorts[0], managementPorts, 0)
            testNode2 = TestNodeFixture(grpcPorts[1], managementPorts, 1)
            testNode3 = TestNodeFixture(grpcPorts[2], managementPorts, 2)
            systems = Arrays.asList(testNode1!!.system, testNode2!!.system, testNode3!!.system)
            val springContext = SpringIntegration.applicationContext(testNode1!!.system)
            val transactionManager = springContext.getBean(JpaTransactionManager::class.java)
            // create schemas
            createTables(transactionManager, testNode1!!.system)
            init(testNode1!!.testKit.system(), orderServiceClient(testNode1!!.testKit.system()))
            init(testNode2!!.testKit.system(), orderServiceClient(testNode1!!.testKit.system()))
            init(testNode3!!.testKit.system(), orderServiceClient(testNode1!!.testKit.system()))

            // wait for all nodes to have joined the cluster, become up and see all other nodes as up
            val upProbe = testNode1!!.testKit.createTestProbe<Any>()
            systems?.forEach(
                Consumer { system: ActorSystem<*>? ->
                    upProbe.awaitAssert(Duration.ofSeconds(15)) {
                        val cluster = Cluster.get(system)
                        Assert.assertEquals(MemberStatus.up(), cluster.selfMember().status())
                        cluster
                            .state()
                            .members
                            .iterator()
                            .forEachRemaining { member: Member ->
                                Assert.assertEquals(
                                    MemberStatus.up(),
                                    member.status()
                                )
                            }
                    }
                })
        }

        @AfterClass
        @JvmStatic
        fun tearDown() {
            testNode3?.testKit?.shutdownTestKit()
            testNode2?.testKit?.shutdownTestKit()
            testNode1?.testKit?.shutdownTestKit()
        }
    }

    private class TestNodeFixture(grpcPort: Int, managementPorts: List<Int>, managementPortIndex: Int) {
        val testKit: ActorTestKit =
            ActorTestKit.create(
                "IntegrationTest",
                nodeConfig(grpcPort, managementPorts, managementPortIndex)
                    .withFallback(sharedConfig())
            )

        val system: ActorSystem<*> = testKit.system()
        private val clientSettings: GrpcClientSettings =
            GrpcClientSettings.connectToServiceAt("127.0.0.1", grpcPort, system).withTls(false)
        private var client: ShoppingCartServiceClient? = null

        fun getClient(): ShoppingCartService? {
            if (client == null) {
                client = ShoppingCartServiceClient.create(clientSettings, system)
                CoordinatedShutdown.get(system)
                    .addTask(
                        CoordinatedShutdown.PhaseBeforeServiceUnbind(),
                        "close-test-client-for-grpc",
                        Supplier { client?.close() })
            }
            return client
        }
    }

    @Test
    fun updateFromDifferentNodesViaGrpc() {
        // add from client1
        val response1 =
            testNode1?.getClient()?.addItem(
                AddItemRequest.newBuilder()
                    .setCartId("cart-1")
                    .setItemId("foo")
                    .setQuantity(42)
                    .build()
            )
        val updatedCart1 = response1!!.toCompletableFuture()[requestTimeout.seconds, TimeUnit.SECONDS]
        Assert.assertEquals("foo", updatedCart1.getItems(0).itemId)
        Assert.assertEquals(42, updatedCart1.getItems(0).quantity.toLong())

        // add from client2
        val response2 =
            testNode2?.getClient()?.addItem(
                AddItemRequest.newBuilder()
                    .setCartId("cart-2")
                    .setItemId("bar")
                    .setQuantity(17)
                    .build()
            )
        val updatedCart2 = response2!!.toCompletableFuture()[requestTimeout.seconds, TimeUnit.SECONDS]
        Assert.assertEquals("bar", updatedCart2.getItems(0).itemId)
        Assert.assertEquals(17, updatedCart2.getItems(0).quantity.toLong())
    }
}