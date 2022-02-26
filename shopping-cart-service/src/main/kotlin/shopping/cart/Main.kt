package shopping.cart

import akka.actor.typed.ActorSystem
import akka.actor.typed.javadsl.Behaviors
import akka.grpc.GrpcClientSettings
import akka.management.cluster.bootstrap.ClusterBootstrap
import akka.management.javadsl.AkkaManagement
import org.slf4j.LoggerFactory
import org.springframework.orm.jpa.JpaTransactionManager
import shopping.cart.repository.ItemPopularityRepository
import shopping.cart.repository.SpringIntegration.Companion.applicationContext
import shopping.order.proto.ShoppingOrderService
import shopping.order.proto.ShoppingOrderServiceClient

object Main {
    private val logger = LoggerFactory.getLogger(Main::class.java)

    @JvmStatic
    fun main(args: Array<String>) {
        val system = ActorSystem.create(Behaviors.empty<Void>(), "ShoppingCartService")

        try {
            init(system, orderServiceClient(system))
        } catch (e: Exception) {
            logger.error("Terminating due to initialization failure.", e)
            system.terminate()
        }
    }

    @JvmStatic
    fun init(system: ActorSystem<Void>, orderService: ShoppingOrderService) {
        AkkaManagement.get(system).start()
        ClusterBootstrap.get(system).start()

        ShoppingCart.init(system)

        val springContext = applicationContext(system)

        val itemPopularityRepository = springContext.getBean(ItemPopularityRepository::class.java)

        val transactionManager = springContext.getBean(JpaTransactionManager::class.java)

        ItemPopularityProjection.init(system, transactionManager, itemPopularityRepository)
        PublishEventsProjection.init(system, transactionManager)

        SendOrderProjection.init(system, transactionManager, orderService)

        startGrpcServer(system, itemPopularityRepository)
    }

    private fun startGrpcServer(system: ActorSystem<Void>, itemPopularityRepository: ItemPopularityRepository) {
        // grpc shopping cart server init
        val config = system.settings().config()
        val grpcInterface = config.getString("shopping-cart-service.grpc.interface")
        val grpcPort = config.getInt("shopping-cart-service.grpc.port")

        val grpcService = ShoppingCartServiceImpl(system, itemPopularityRepository)

        ShoppingCartServer.start(grpcInterface, grpcPort, system, grpcService)
    }

    fun orderServiceClient(system: ActorSystem<*>): ShoppingOrderService {
        val orderServiceClientSettings =
            GrpcClientSettings.connectToServiceAt(
                system.settings().config().getString("shopping-order-service.host"),
                system.settings().config().getInt("shopping-order-service.port"),
                system
            ).withTls(false)

        return ShoppingOrderServiceClient.create(orderServiceClientSettings, system)
    }
}
