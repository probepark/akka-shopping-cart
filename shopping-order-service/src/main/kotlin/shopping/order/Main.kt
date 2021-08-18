package shopping.order

import akka.actor.typed.ActorSystem
import akka.actor.typed.javadsl.Behaviors
import akka.management.cluster.bootstrap.ClusterBootstrap
import akka.management.javadsl.AkkaManagement
import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import shopping.order.proto.ShoppingOrderService

object Main {
    private val logger = LoggerFactory.getLogger(Main::class.java)

    @JvmStatic
    fun main(args: Array<String>) {
        val system = ActorSystem.create(Behaviors.empty<Void>(), "ShoppingOrderService")
        try {
            init(system)
        } catch (e: Exception) {
            logger.error("Terminating due to initialization failure.", e)
            system.terminate()
        }
    }

    @JvmStatic
    fun init(system: ActorSystem<Void>?) {
        AkkaManagement.get(system).start()
        ClusterBootstrap.get(system).start()

        val config: Config = system!!.settings().config()
        val grpcInterface: String = config.getString("shopping-order-service.grpc.interface")
        val grpcPort: Int = config.getInt("shopping-order-service.grpc.port")
        val grpcService: ShoppingOrderService = ShoppingOrderServiceImpl()
        ShoppingOrderServer.start(grpcInterface, grpcPort, system, grpcService)
    }
}