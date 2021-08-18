package shopping.cart

import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.DispatcherSelector
import akka.cluster.sharding.typed.javadsl.ClusterSharding
import akka.cluster.sharding.typed.javadsl.EntityRef
import akka.grpc.GrpcServiceException
import akka.pattern.StatusReply
import io.grpc.Status
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import shopping.cart.ShoppingCart.Checkout
import shopping.cart.proto.*
import shopping.cart.repository.ItemPopularityRepository
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.TimeoutException


class ShoppingCartServiceImpl(system: ActorSystem<*>, private val itemPopularityRepository: ItemPopularityRepository) :
        ShoppingCartService {

    private val logger: Logger = LoggerFactory.getLogger(javaClass)

    private val dispatcherSelector = DispatcherSelector.fromConfig("akka.projection.jdbc.blocking-jdbc-dispatcher")
    private val blockingJdbcExecutor = system.dispatchers().lookup(dispatcherSelector)
    private val timeout = system.settings().config().getDuration("shopping-cart-service.ask-timeout")
    private val sharding = ClusterSharding.get(system)

    companion object {

        private fun toProtoCart(cart: ShoppingCart.Summary): Cart {
            val protoItems =
                    cart.items
                            .map { Item.newBuilder().setItemId(it.key).setQuantity(it.value).build() }
                            .toList()

            return Cart.newBuilder().setCheckedOut(cart.checkedOut).addAllItems(protoItems).build()
        }

        private fun <T> convertError(response: CompletionStage<T>): CompletionStage<T> {
            return response.exceptionally { ex: Throwable ->
                when (ex) {
                    is TimeoutException -> {
                        throw GrpcServiceException(Status.UNAVAILABLE.withDescription("Operation timed out"))
                    }
                    else -> {
                        throw GrpcServiceException(Status.INVALID_ARGUMENT.withDescription(ex.message))
                    }
                }
            }
        }
    }

    override fun addItem(addItemRequest: AddItemRequest): CompletionStage<Cart> {

        logger.info("adding Item {} to cart {}", addItemRequest.itemId, addItemRequest.cartId)

        val entityRef: EntityRef<ShoppingCart.Command> = sharding.entityRefFor(ShoppingCart.ENTITY_KEY, addItemRequest.cartId)
        val reply: CompletionStage<ShoppingCart.Summary> =
                entityRef.askWithStatus(
                        { replyTo ->
                            ShoppingCart.AddItem(addItemRequest.itemId, addItemRequest.quantity, replyTo)
                        }, timeout)
        val cart: CompletionStage<Cart> = reply.thenApply(ShoppingCartServiceImpl::toProtoCart)

        return convertError(cart)
    }

    override fun removeItem(removeItemRequest: RemoveItemRequest): CompletionStage<Cart> {

        logger.info("removing Item {} from cart {}", removeItemRequest.itemId, removeItemRequest.cartId)

        val entityRef: EntityRef<ShoppingCart.Command> = sharding.entityRefFor(ShoppingCart.ENTITY_KEY, removeItemRequest.cartId)

        val reply: CompletionStage<ShoppingCart.Summary> =
                entityRef.askWithStatus({ replyTo ->
                    ShoppingCart.RemoveItem(removeItemRequest.itemId, replyTo)
                }, timeout)

        val cart: CompletionStage<Cart> = reply.thenApply(ShoppingCartServiceImpl::toProtoCart)

        return convertError(cart)
    }

    override fun adjustItemQuantity(adjustItemQuantityRequest: AdjustItemQuantityRequest): CompletionStage<Cart> {

        logger.info("adjusting Item quantity for item {} from cart {}", adjustItemQuantityRequest.itemId, adjustItemQuantityRequest.cartId)

        val entityRef: EntityRef<ShoppingCart.Command> = sharding.entityRefFor(ShoppingCart.ENTITY_KEY, adjustItemQuantityRequest.cartId)

        val reply: CompletionStage<ShoppingCart.Summary> =
                entityRef.askWithStatus({ replyTo ->
                    ShoppingCart.AdjustItemQuantity(adjustItemQuantityRequest.itemId, adjustItemQuantityRequest.quantity, replyTo)
                }, timeout)

        val cart: CompletionStage<Cart> = reply.thenApply(ShoppingCartServiceImpl::toProtoCart)

        return convertError(cart)
    }

    override fun checkout(checkoutRequest: CheckoutRequest): CompletionStage<Cart> {

        logger.info("checkout {}", checkoutRequest.cartId)

        val entityRef = sharding.entityRefFor(ShoppingCart.ENTITY_KEY, checkoutRequest.cartId)
        val reply = entityRef.askWithStatus({ replyTo: ActorRef<StatusReply<ShoppingCart.Summary>> -> Checkout(replyTo) }, timeout)
        val cart = reply.thenApply(ShoppingCartServiceImpl::toProtoCart)

        return convertError(cart)
    }

    override fun getCart(getCartRequest: GetCartRequest): CompletionStage<Cart> {

        logger.info("getCart {}", getCartRequest.cartId)

        val entityRef = sharding.entityRefFor(ShoppingCart.ENTITY_KEY, getCartRequest.cartId)
        val reply = entityRef.ask({ replyTo: ActorRef<ShoppingCart.Summary> -> ShoppingCart.Get(replyTo) }, timeout)
        val cart = reply.thenApply { summary: ShoppingCart.Summary ->
            if (summary.items.isEmpty()) throw GrpcServiceException(
                    Status.NOT_FOUND.withDescription("Cart " + getCartRequest.cartId.toString() + " not found")) else return@thenApply toProtoCart(summary)
        }

        return convertError(cart)
    }

    override fun getItemPopularity(getItemPopularityRequest: GetItemPopularityRequest): CompletionStage<GetItemPopularityResponse> {

        logger.info("getItemPopularity for item {}", getItemPopularityRequest.itemId)

        val itemPopularity = CompletableFuture.supplyAsync(
                { itemPopularityRepository.findById(getItemPopularityRequest.itemId) }, blockingJdbcExecutor)

        return itemPopularity.thenApply { popularity ->
            val count: Long = popularity.map(ItemPopularity::count).orElse(0L)
            GetItemPopularityResponse.newBuilder().setPopularityCount(count).build()
        }
    }
}