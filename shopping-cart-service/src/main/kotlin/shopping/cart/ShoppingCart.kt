package shopping.cart

import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.SupervisorStrategy
import akka.actor.typed.javadsl.Behaviors
import akka.cluster.sharding.typed.javadsl.ClusterSharding
import akka.cluster.sharding.typed.javadsl.Entity
import akka.cluster.sharding.typed.javadsl.EntityTypeKey
import akka.pattern.StatusReply
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.javadsl.CommandHandlerWithReply
import akka.persistence.typed.javadsl.CommandHandlerWithReplyBuilderByState
import akka.persistence.typed.javadsl.EventHandler
import akka.persistence.typed.javadsl.EventSourcedBehavior
import akka.persistence.typed.javadsl.EventSourcedBehaviorWithEnforcedReplies
import akka.persistence.typed.javadsl.ReplyEffect
import akka.persistence.typed.javadsl.RetentionCriteria
import com.fasterxml.jackson.annotation.JsonCreator
import shopping.cart.ShoppingCart.Command
import shopping.cart.ShoppingCart.Event
import shopping.cart.ShoppingCart.State
import java.time.Duration.ofMillis
import java.time.Duration.ofSeconds
import java.time.Instant
import java.util.Optional
import kotlin.math.abs

class ShoppingCart private constructor(private val cartId: String, private val projectionTag: String) :
    EventSourcedBehaviorWithEnforcedReplies<Command, Event, State>(
        PersistenceId.of(ENTITY_KEY.name(), cartId),
        SupervisorStrategy.restartWithBackoff(ofMillis(200), ofSeconds(5), 0.1)
    ) {

    companion object {
        val ENTITY_KEY: EntityTypeKey<Command> = EntityTypeKey.create(Command::class.java, "ShoppingCart")
        val TAGS = listOf("carts-0", "carts-1", "carts-2", "carts-3", "carts-4")

        fun init(system: ActorSystem<*>) {
            ClusterSharding.get(system).init(
                Entity.of(ENTITY_KEY) { entityContext ->
                    val selectedTag = getSelectedTag(entityContext.entityId)
                    create(entityContext.entityId, selectedTag)
                }
            )
        }

        fun create(cartId: String, projectionTag: String = getSelectedTag(cartId)): Behavior<Command> {
            return Behaviors.setup { ctx ->
                EventSourcedBehavior
                    .start(ShoppingCart(cartId, projectionTag), ctx)
            }
        }

        private fun getSelectedTag(entityId: String): String {
            val index = abs(entityId.hashCode() % TAGS.size)
            return TAGS[index]
        }
    }

    // commands
    sealed interface Command : CborSerializable

    data class AddItem(val itemId: String, val quantity: Int, val replyTo: ActorRef<StatusReply<Summary>>) : Command

    data class RemoveItem(val itemId: String, val replyTo: ActorRef<StatusReply<Summary>>) : Command

    data class AdjustItemQuantity(val itemId: String, val quantity: Int, val replyTo: ActorRef<StatusReply<Summary>>) :
        Command

    data class Checkout @JsonCreator constructor(val replyTo: ActorRef<StatusReply<Summary>>) : Command

    data class Get @JsonCreator constructor(val replyTo: ActorRef<Summary>) : Command

    data class Summary @JsonCreator constructor(val items: Map<String, Int>, val checkedOut: Boolean = false) :
        CborSerializable

    // events
    sealed class Event(open val cartId: String) : CborSerializable

    data class ItemAdded(override val cartId: String, val itemId: String, val quantity: Int) : Event(cartId)

    data class ItemRemoved(override val cartId: String, val itemId: String) : Event(cartId)

    data class ItemQuantityAdjusted(override val cartId: String, val itemId: String, val quantity: Int) : Event(cartId)

    data class CheckedOut(override val cartId: String, val eventTime: Instant) : Event(cartId)

    // state
    class State(
        private val items: MutableMap<String, Int> = mutableMapOf(),
        private var checkoutDate: Optional<Instant> = Optional.empty()
    ) : CborSerializable {

        fun itemExists(itemId: String): Boolean = items.containsKey(itemId)

        fun itemDoesNotExist(itemId: String): Boolean = !itemExists(itemId)

        fun updateItemQuantity(itemId: String, quantity: Int): State {
            if (quantity == 0) items.remove(itemId)
            else items[itemId] = quantity

            return this
        }

        fun toSummary(): Summary = Summary(items, isCheckedOut())

        fun itemCount(itemId: String): Int? = items[itemId]

        fun isEmpty(): Boolean = items.isEmpty()

        fun isCheckedOut(): Boolean {
            return checkoutDate.isPresent
        }

        fun checkout(now: Instant): State {
            checkoutDate = Optional.of(now)

            return this
        }
    }

    override fun tagsFor(event: Event): Set<String> {
        return setOf(projectionTag)
    }

    override fun commandHandler(): CommandHandlerWithReply<Command, Event, State> {
        return openShoppingCart()
            .orElse(checkedOutShoppingCart())
            .orElse(getCommandHandler())
            .build()
    }

    override fun eventHandler(): EventHandler<State, Event> {
        return newEventHandlerBuilder()
            .forAnyState()
            .onEvent(ItemAdded::class.java) { state, event -> state.updateItemQuantity(event.itemId, event.quantity) }
            .onEvent(ItemRemoved::class.java) { state, event -> state.updateItemQuantity(event.itemId, 0) }
            .onEvent(ItemQuantityAdjusted::class.java) { state, event ->
                state.updateItemQuantity(
                    event.itemId,
                    event.quantity
                )
            }
            .onEvent(CheckedOut::class.java) { state, event -> state.checkout(event.eventTime) }
            .build()
    }

    override fun retentionCriteria(): RetentionCriteria {
        return RetentionCriteria.snapshotEvery(100, 3)
    }

    override fun emptyState(): State {
        return State()
    }

    private fun openShoppingCart(): CommandHandlerWithReplyBuilderByState<Command, Event, State, State> {
        return newCommandHandlerWithReplyBuilder()
            .forState { state -> !state.isCheckedOut() }
            .onCommand(AddItem::class.java, this::onAddItem)
            .onCommand(RemoveItem::class.java, this::onRemoveItem)
            .onCommand(AdjustItemQuantity::class.java, this::onAdjustItemQuantity)
            .onCommand(Checkout::class.java, this::onCheckout)
    }

    private fun checkedOutShoppingCart(): CommandHandlerWithReplyBuilderByState<Command, Event, State, State> {
        return newCommandHandlerWithReplyBuilder()
            .forState(State::isCheckedOut)
            .onCommand(AddItem::class.java) { cmd ->
                Effect().reply(
                    cmd.replyTo,
                    StatusReply.error("Can't add an item to an already checked out shopping cart")
                )
            }
            .onCommand(RemoveItem::class.java) { cmd ->
                Effect().reply(
                    cmd.replyTo,
                    StatusReply.error("Can't remove an item from an already checked out shopping cart")
                )
            }
            .onCommand(AdjustItemQuantity::class.java) { cmd ->
                Effect().reply(
                    cmd.replyTo,
                    StatusReply.error("Can't adjust the quantity of an item from an already checked out shopping cart")
                )
            }
            .onCommand(Checkout::class.java) { cmd ->
                Effect().reply(cmd.replyTo, StatusReply.error("Can't checkout already checked out shopping cart"))
            }
    }

    private fun getCommandHandler(): CommandHandlerWithReplyBuilderByState<Command, Event, State, State>? {
        return newCommandHandlerWithReplyBuilder()
            .forAnyState()
            .onCommand(Get::class.java, this::onGetItem)
    }

    private fun onAddItem(state: State, cmd: AddItem): ReplyEffect<Event, State> {
        return when {
            state.itemExists(cmd.itemId) -> {
                Effect().reply(
                    cmd.replyTo,
                    StatusReply.error("Item '${cmd.itemId}' was already added to this shopping cart")
                )
            }
            cmd.quantity <= 0 -> {
                Effect().reply(cmd.replyTo, StatusReply.error("Quantity must be greater than zero"))
            }
            else -> {
                Effect()
                    .persist(ItemAdded(cartId, cmd.itemId, cmd.quantity))
                    .thenReply(cmd.replyTo) { updatedCart -> StatusReply.success(updatedCart.toSummary()) }
            }
        }
    }

    private fun onRemoveItem(state: State, cmd: RemoveItem): ReplyEffect<Event, State> {
        return when {
            state.itemDoesNotExist(cmd.itemId) -> {
                Effect().reply(
                    cmd.replyTo,
                    StatusReply.error("Item '${cmd.itemId}' does not exist in the shopping cart")
                )
            }
            else -> {
                Effect()
                    .persist(ItemRemoved(cartId, cmd.itemId))
                    .thenReply(cmd.replyTo) { updatedCart -> StatusReply.success(updatedCart.toSummary()) }
            }
        }
    }

    private fun onAdjustItemQuantity(state: State, cmd: AdjustItemQuantity): ReplyEffect<Event, State> {
        return when {
            state.itemDoesNotExist(cmd.itemId) -> {
                Effect().reply(
                    cmd.replyTo,
                    StatusReply.error("Item '${cmd.itemId}' does not exist in the shopping cart")
                )
            }
            cmd.quantity <= 0 -> {
                Effect().reply(cmd.replyTo, StatusReply.error("Quantity must be greater than zero"))
            }
            else -> {
                Effect()
                    .persist(ItemQuantityAdjusted(cartId, cmd.itemId, cmd.quantity))
                    .thenReply(cmd.replyTo) { updatedCart -> StatusReply.success(updatedCart.toSummary()) }
            }
        }
    }

    private fun onCheckout(state: State, cmd: Checkout): ReplyEffect<Event, State> {
        return when {
            state.isEmpty() -> Effect().reply(cmd.replyTo, StatusReply.error("Cannot checkout an empty shopping cart"))

            else -> {
                Effect()
                    .persist(CheckedOut(cartId, Instant.now()))
                    .thenReply(cmd.replyTo) { updatedCart -> StatusReply.success(updatedCart.toSummary()) }
            }
        }
    }

    private fun onGetItem(state: State, cmd: Get): ReplyEffect<Event, State> =
        Effect().reply(cmd.replyTo, state.toSummary())
}
