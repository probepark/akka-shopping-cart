package shopping.analytics

import akka.Done
import akka.actor.typed.ActorSystem
import akka.kafka.CommitterSettings
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.ConsumerSettings
import akka.kafka.Subscriptions
import akka.kafka.javadsl.Committer
import akka.kafka.javadsl.Consumer
import akka.stream.RestartSettings
import akka.stream.javadsl.RestartSource
import com.google.protobuf.Any
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import shopping.cart.proto.CheckedOut
import shopping.cart.proto.ItemAdded
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage

sealed class ShoppingCartEventConsumer {

    companion object {

        private val log: Logger = LoggerFactory.getLogger(ShoppingCartEventConsumer::class.java)

        @JvmStatic
        fun init(system: ActorSystem<*>) {
            val topic =
                system
                    .settings()
                    .config()
                    .getString("shopping-analytics-service.shopping-cart-kafka-topic")

            val consumerSettings =
                ConsumerSettings.create(system, StringDeserializer(), ByteArrayDeserializer())
                    .withGroupId("shopping-cart-analytics")
            val committerSettings = CommitterSettings.create(system)
            val minBackoff = Duration.ofSeconds(1)
            val maxBackoff = Duration.ofSeconds(30)
            val randomFactor = 0.1
            RestartSource
                .onFailuresWithBackoff(
                    RestartSettings.create(minBackoff, maxBackoff, randomFactor)
                ) {
                    Consumer.committableSource(
                        consumerSettings, Subscriptions.topics(topic)
                    )
                        .mapAsync(
                            1
                        ) { msg: CommittableMessage<String, ByteArray> -> handleRecord(msg.record()).thenApply { msg.committableOffset() } }
                        .via(Committer.flow(committerSettings))
                }
                .run(system)
        }

        private fun handleRecord(record: ConsumerRecord<String, ByteArray>): CompletionStage<Done> {
            val bytes = record.value()
            val x = Any.parseFrom(bytes)
            val typeUrl = x.typeUrl
            val inputBytes = x.value.newCodedInput()
            try {
                when (typeUrl) {
                    "shopping-cart-service/shoppingcart.ItemAdded" -> {
                        val event: ItemAdded = ItemAdded.parseFrom(inputBytes)
                        log.info(
                            "ItemAdded: {} {} to cart {}",
                            event.quantity,
                            event.itemId,
                            event.cartId
                        )
                    }
                    "shopping-cart-service/shoppingcart.CheckedOut" -> {
                        val event: CheckedOut = CheckedOut.parseFrom(inputBytes)
                        log.info("CheckedOut: cart {} checked out", event.cartId)
                    }
                    else -> throw IllegalArgumentException("unknown record type $typeUrl")
                }
            } catch (e: Exception) {
                log.error("Could not process event of type [{}]", typeUrl, e)
                // continue with next
            }
            return CompletableFuture.completedFuture(Done.getInstance())
        }
    }
}
