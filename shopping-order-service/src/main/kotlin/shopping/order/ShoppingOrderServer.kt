package shopping.order

import akka.actor.typed.ActorSystem
import akka.grpc.javadsl.ServerReflection
import akka.grpc.javadsl.ServiceHandler
import akka.http.javadsl.Http
import akka.http.javadsl.ServerBinding
import akka.http.javadsl.model.HttpRequest
import akka.http.javadsl.model.HttpResponse
import akka.japi.function.Function
import shopping.order.proto.ShoppingOrderService
import shopping.order.proto.ShoppingOrderServiceHandlerFactory
import java.net.InetSocketAddress
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletionStage

class ShoppingOrderServer {

    companion object {

        fun start(host: String, port: Int, system: ActorSystem<*>, grpcService: ShoppingOrderService) {

            val service: Function<HttpRequest, CompletionStage<HttpResponse>> = ServiceHandler.concatOrNotFound(
                ShoppingOrderServiceHandlerFactory.create(
                    grpcService,
                    system
                ), // ServerReflection enabled to support grpcurl without import-path and proto parameters
                ServerReflection.create(
                    Collections.singletonList(ShoppingOrderService.description), system
                )
            )
            val bound: CompletionStage<ServerBinding> = Http.get(system).newServerAt(host, port).bind(service::apply)

            bound.whenComplete { binding: ServerBinding?, ex: Throwable? ->
                if (binding != null) {
                    binding.addToCoordinatedShutdown(Duration.ofSeconds(3), system)
                    val address: InetSocketAddress = binding.localAddress()
                    system.log().info("Shopping order at gRPC server {}:{}", address.hostString, address.port)
                } else {
                    system.log().error("Failed to bind gRPC endpoint, terminating system", ex)
                    system.terminate()
                }
            }
        }
    }
}
