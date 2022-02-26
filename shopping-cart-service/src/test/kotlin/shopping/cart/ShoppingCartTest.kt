package shopping.cart

import akka.actor.testkit.typed.javadsl.TestKitJunitResource
import akka.pattern.StatusReply
import akka.persistence.testkit.javadsl.EventSourcedBehaviorTestKit
import akka.persistence.testkit.javadsl.EventSourcedBehaviorTestKit.CommandResultWithReply
import com.typesafe.config.ConfigFactory
import org.junit.Before
import org.junit.ClassRule
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

internal class ShoppingCartTest {

    companion object {

        @ClassRule
        @JvmField
        val testKit: TestKitJunitResource = TestKitJunitResource(
            ConfigFactory.parseString("""akka.actor.serialization-bindings {"shopping.cart.CborSerializable" = jackson-cbor}""")
                .withFallback(EventSourcedBehaviorTestKit.config())
        )

        private const val CART_ID = "testCart"

        private val eventSourcedTestKit: EventSourcedBehaviorTestKit<Command, Event, State> =
            EventSourcedBehaviorTestKit.create(testKit.system(), ShoppingCart.create(CART_ID))
    }

    @Before
    fun beforeEach() {
        eventSourcedTestKit.clear()
    }

    @Test
    fun addAnItemToCart() {
        val result: CommandResultWithReply<Command, Event, State, StatusReply<Summary>> =
            eventSourcedTestKit.runCommand { replyTo -> AddItem("foo", 42, replyTo) }

        assertTrue(result.reply().isSuccess)
        val (items) = result.reply().value
        assertEquals(1, items.size)
        assertEquals(42, items["foo"])
        assertEquals(ItemAdded(CART_ID, "foo", 42), result.event())
    }

    @Test
    fun rejectAlreadyAddedItem() {
        val result1: CommandResultWithReply<Command, Event, State, StatusReply<Summary>> =
            eventSourcedTestKit.runCommand { replyTo -> AddItem("foo", 42, replyTo) }

        assertTrue(result1.reply().isSuccess)

        val result2: CommandResultWithReply<Command, Event, State, StatusReply<Summary>> =
            eventSourcedTestKit.runCommand { replyTo -> AddItem("foo", 42, replyTo) }

        assertTrue(result2.reply().isError)
        assertTrue(result2.hasNoEvents())
    }

    @Test
    fun checkout() {
        val result1: CommandResultWithReply<Command, Event, State, StatusReply<Summary>> =
            eventSourcedTestKit.runCommand { replyTo -> AddItem("foo", 42, replyTo) }
        assertTrue(result1.reply().isSuccess)

        val result2: CommandResultWithReply<Command, Event, State, StatusReply<Summary>> =
            eventSourcedTestKit.runCommand { replyTo -> Checkout(replyTo) }
        assertTrue(result2.reply().isSuccess)
        assertTrue(result2.event() is CheckedOut)
        assertEquals(CART_ID, result2.event().cartId)
    }

    @Test
    fun rejectCheckout() {
        val result1: CommandResultWithReply<Command, Event, State, StatusReply<Summary>> =
            eventSourcedTestKit.runCommand { replyTo -> AddItem("foo", 42, replyTo) }
        assertTrue(result1.reply().isSuccess)

        val result2: CommandResultWithReply<Command, Event, State, StatusReply<Summary>> =
            eventSourcedTestKit.runCommand { replyTo -> Checkout(replyTo) }
        assertTrue(result2.reply().isSuccess)
        assertTrue(result2.event() is CheckedOut)
        assertEquals(CART_ID, result2.event().cartId)

        val result3: CommandResultWithReply<Command, Event, State, StatusReply<Summary>> =
            eventSourcedTestKit.runCommand { replyTo -> AddItem("foo", 42, replyTo) }
        assertTrue(result3.reply().isError)
        assertTrue(result3.hasNoEvents())
    }

    @Test
    fun getACart() {
        val result1: CommandResultWithReply<Command, Event, State, StatusReply<Summary>> =
            eventSourcedTestKit.runCommand { replyTo -> AddItem("foo", 42, replyTo) }
        assertTrue(result1.reply().isSuccess)

        val result2: CommandResultWithReply<Command, Event, State, Summary> =
            eventSourcedTestKit.runCommand { replyTo -> Get(replyTo) }
        assertFalse(result2.reply().checkedOut)
        assertEquals(1, result2.reply().items.size)
        assertEquals(42, result2.reply().items["foo"]!!.toInt())
    }

    @Test
    fun getACheckedOutCart() {
        val result1: CommandResultWithReply<Command, Event, State, StatusReply<Summary>> =
            eventSourcedTestKit.runCommand { replyTo -> AddItem("foo", 42, replyTo) }
        assertTrue(result1.reply().isSuccess)

        val result2: CommandResultWithReply<Command, Event, State, StatusReply<Summary>> =
            eventSourcedTestKit.runCommand { replyTo -> Checkout(replyTo) }
        assertTrue(result2.reply().isSuccess)

        val result3: CommandResultWithReply<Command, Event, State, Summary> =
            eventSourcedTestKit.runCommand { replyTo -> Get(replyTo) }
        assertTrue(result3.reply().checkedOut)
        assertEquals(1, result3.reply().items.size)
        assertEquals(42, result3.reply().items["foo"]!!.toInt())
    }

    @Test
    fun removeAnItemFromCart() {
        val result1: CommandResultWithReply<Command, Event, State, StatusReply<Summary>> =
            eventSourcedTestKit.runCommand { replyTo -> AddItem("foo", 42, replyTo) }
        assertTrue(result1.reply().isSuccess)

        val result2: CommandResultWithReply<Command, Event, State, StatusReply<Summary>> =
            eventSourcedTestKit.runCommand { replyTo -> RemoveItem("foo", replyTo) }
        assertTrue(result2.reply().isSuccess)

        assertTrue(result2.event() is ItemRemoved)
        assertEquals(CART_ID, result2.event().cartId)
    }

    @Test
    fun adjustAnItemQuantityInCart() {
        val quantity1 = 42
        val result1: CommandResultWithReply<Command, Event, State, StatusReply<Summary>> =
            eventSourcedTestKit.runCommand { replyTo -> AddItem("foo", quantity1, replyTo) }
        assertTrue(result1.reply().isSuccess)

        val (items1) = result1.reply().value
        assertEquals(1, items1.size)
        assertEquals(quantity1, items1["foo"])
        assertEquals(ItemAdded(CART_ID, "foo", quantity1), result1.event())

        val quantity2 = 35
        val result2: CommandResultWithReply<Command, Event, State, StatusReply<Summary>> =
            eventSourcedTestKit.runCommand { replyTo -> AdjustItemQuantity("foo", quantity2, replyTo) }
        assertTrue(result2.reply().isSuccess)

        val (items2) = result2.reply().value
        assertEquals(1, items2.size)
        assertEquals(quantity2, items2["foo"])
        assertEquals(ItemQuantityAdjusted(CART_ID, "foo", quantity2), result2.event())
    }

    @Test
    fun adjustAnItemQuantityInCartWithZeroQuantity() {
        val quantity1 = 42
        val result1: CommandResultWithReply<Command, Event, State, StatusReply<Summary>> =
            eventSourcedTestKit.runCommand { replyTo -> AddItem("foo", quantity1, replyTo) }
        assertTrue(result1.reply().isSuccess)

        val (items1) = result1.reply().value
        assertEquals(1, items1.size)
        assertEquals(quantity1, items1["foo"])
        assertEquals(ItemAdded(CART_ID, "foo", quantity1), result1.event())

        val quantity2 = 0
        val result2: CommandResultWithReply<Command, Event, State, StatusReply<Summary>> =
            eventSourcedTestKit.runCommand { replyTo -> AdjustItemQuantity("foo", quantity2, replyTo) }
        assertTrue(result2.reply().isError)
        assertTrue(result2.hasNoEvents())
    }
}
