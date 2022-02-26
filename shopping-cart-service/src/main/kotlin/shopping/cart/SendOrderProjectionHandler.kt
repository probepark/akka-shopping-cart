package shopping.cart

import akka.Done
import akka.Done.done
import akka.actor.typed.ActorSystem
import akka.cluster.sharding.typed.javadsl.ClusterSharding
import akka.cluster.sharding.typed.javadsl.EntityRef
import akka.projection.eventsourced.EventEnvelope
import akka.projection.javadsl.Handler
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import shopping.order.proto.Item
import shopping.order.proto.OrderRequest
import shopping.order.proto.ShoppingOrderService
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.stream.Collectors

class SendOrderProjectionHandler(system: ActorSystem<*>, private val orderService: ShoppingOrderService) :
    Handler<EventEnvelope<ShoppingCart.Event>>() {

    private val log: Logger = LoggerFactory.getLogger(javaClass)
    private val sharding: ClusterSharding = ClusterSharding.get(system)
    private val timeout: Duration = system.settings().config().getDuration("shopping-cart-service.ask-timeout")

    //    @Throws(Exception::class)
    override fun process(envelope: EventEnvelope<ShoppingCart.Event>): CompletionStage<Done> {
        return if (envelope.event() is ShoppingCart.CheckedOut) {
            val checkedOut = envelope.event() as ShoppingCart.CheckedOut
            sendOrder(checkedOut)
        } else {
            CompletableFuture.completedFuture(done())
        }
    }

    private fun sendOrder(checkout: ShoppingCart.CheckedOut): CompletionStage<Done> {
        val entityRef: EntityRef<ShoppingCart.Command> = sharding.entityRefFor(ShoppingCart.ENTITY_KEY, checkout.cartId)
        val reply: CompletionStage<ShoppingCart.Summary> =
            entityRef.ask({ replyTo -> ShoppingCart.Get(replyTo) }, timeout)

        return reply.thenCompose { (items) ->
            val protoItems: List<Item> = items.entries.stream()
                .map { (key, value) ->
                    Item.newBuilder()
                        .setItemId(key)
                        .setQuantity(value)
                        .build()
                }
                .collect(Collectors.toList())
            log.info("Sending order of {} items for cart {}.", items.size, checkout.cartId)
            val orderRequest: OrderRequest =
                OrderRequest.newBuilder().setCartId(checkout.cartId).addAllItems(protoItems).build()
            orderService.order(orderRequest).thenApply { done() }
        }
    }
}
