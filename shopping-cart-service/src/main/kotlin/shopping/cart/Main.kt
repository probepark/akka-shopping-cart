package shopping.cart

import akka.actor.typed.ActorSystem
import akka.actor.typed.javadsl.Behaviors
import akka.management.cluster.bootstrap.ClusterBootstrap
import akka.management.javadsl.AkkaManagement
import org.slf4j.LoggerFactory
import org.springframework.orm.jpa.JpaTransactionManager
import shopping.cart.repository.ItemPopularityRepository
import shopping.cart.repository.SpringIntegration.Companion.applicationContext


object Main {
    private val logger = LoggerFactory.getLogger(Main::class.java)

    @JvmStatic
    fun main(args: Array<String>) {
        val system = ActorSystem.create(Behaviors.empty<Void>(), "ShoppingCartService")

        try {
            init(system)
        } catch (e: Exception) {
            logger.error("Terminating due to initialization failure.", e)
            system.terminate()
        }
    }

    @JvmStatic
    fun init(system: ActorSystem<Void>) {
        AkkaManagement.get(system).start()
        ClusterBootstrap.get(system).start()

        ShoppingCart.init(system)

        val springContext = applicationContext(system)

        val itemPopularityRepository = springContext.getBean(ItemPopularityRepository::class.java)

        val transactionManager = springContext.getBean(JpaTransactionManager::class.java)

        ItemPopularityProjection.init(system, transactionManager, itemPopularityRepository)
        PublishEventsProjection.init(system, transactionManager);

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
}