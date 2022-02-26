package shopping.cart

import akka.Done
import akka.Done.done
import akka.kafka.javadsl.SendProducer
import akka.projection.eventsourced.EventEnvelope
import akka.projection.javadsl.Handler
import com.google.protobuf.Any
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import shopping.cart.proto.CheckedOut
import shopping.cart.proto.ItemAdded
import java.util.concurrent.CompletionStage

class PublishEventsProjectionHandler(
    private val topic: String,
    private val sendProducer: SendProducer<String, ByteArray>
) :
    Handler<EventEnvelope<ShoppingCart.Event>>() {

    private val logger: Logger = LoggerFactory.getLogger(javaClass)

    override fun process(envelope: EventEnvelope<ShoppingCart.Event>): CompletionStage<Done> {

        val event = envelope.event()

        // using the cartId as the key and `DefaultPartitioner` will select partition based on the key
        // so that events for same cart always ends up in same partition

        // using the cartId as the key and `DefaultPartitioner` will select partition based on the key
        // so that events for same cart always ends up in same partition
        val key = event.cartId
        val producerRecord: ProducerRecord<String, ByteArray> = ProducerRecord(topic, key, serialize(event))

        return sendProducer
            .send(producerRecord)
            .thenApply { recordMetadata ->
                logger.info(
                    "Published event [{}] to topic/partition {}/{}",
                    event,
                    topic,
                    recordMetadata.partition()
                )
                done()
            }
    }

    private fun serialize(event: ShoppingCart.Event): ByteArray {

        return when (event) {
            is ShoppingCart.ItemAdded -> {
                val (cartId, itemId, quantity) = event

                // pack in Any so that type information is included for deserialization
                Any.newBuilder()
                    .setValue(
                        ItemAdded.newBuilder()
                            .setCartId(cartId)
                            .setItemId(itemId)
                            .setQuantity(quantity)
                            .build()
                            .toByteString()
                    )
                    .setTypeUrl("shopping-cart-service/${ItemAdded.getDescriptor().fullName}")
                    .build()
                    .toByteArray()
            }
            is ShoppingCart.CheckedOut -> {
                val (cartId, _) = event

                // pack in Any so that type information is included for deserialization
                Any.newBuilder()
                    .setValue(
                        CheckedOut.newBuilder()
                            .setCartId(cartId)
                            .build()
                            .toByteString()
                    )
                    .setTypeUrl("shopping-cart-service/${CheckedOut.getDescriptor().fullName}")
                    .build()
                    .toByteArray()
            }
            else -> {
                throw IllegalArgumentException("Unknown event type: " + event.javaClass)
            }
        }
    }
}
