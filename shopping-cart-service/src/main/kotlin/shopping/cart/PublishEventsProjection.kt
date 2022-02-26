package shopping.cart

import akka.actor.CoordinatedShutdown
import akka.actor.typed.ActorSystem
import akka.cluster.sharding.typed.ShardedDaemonProcessSettings
import akka.cluster.sharding.typed.javadsl.ShardedDaemonProcess
import akka.kafka.ProducerSettings
import akka.kafka.javadsl.SendProducer
import akka.persistence.jdbc.query.javadsl.JdbcReadJournal
import akka.persistence.query.Offset
import akka.projection.ProjectionBehavior
import akka.projection.ProjectionId
import akka.projection.eventsourced.EventEnvelope
import akka.projection.eventsourced.javadsl.EventSourcedProvider
import akka.projection.javadsl.AtLeastOnceProjection
import akka.projection.javadsl.SourceProvider
import akka.projection.jdbc.javadsl.JdbcProjection
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.orm.jpa.JpaTransactionManager
import shopping.cart.repository.HibernateJdbcSession
import java.util.Optional
import java.util.function.Supplier

sealed class PublishEventsProjection {

    companion object {

        @JvmStatic
        fun init(system: ActorSystem<*>, transactionManager: JpaTransactionManager) {

            val sendProducer: SendProducer<String, ByteArray> = createProducer(system)
            val topic: String = system.settings().config().getString("shopping-cart-service.kafka.topic")
            ShardedDaemonProcess.get(system)
                .init(
                    ProjectionBehavior.Command::class.java,
                    "PublishEventsProjection",
                    ShoppingCart.TAGS.size,
                    { index ->
                        ProjectionBehavior.create(
                            createProjectionFor(system, transactionManager, topic, sendProducer, index)
                        )
                    },
                    ShardedDaemonProcessSettings.create(system),
                    Optional.of(ProjectionBehavior.stopMessage())
                )
        }

        @JvmStatic
        private fun createProducer(system: ActorSystem<*>): SendProducer<String, ByteArray> {

            val producerSettings: ProducerSettings<String, ByteArray> =
                ProducerSettings.create(system, StringSerializer(), ByteArraySerializer())
            val sendProducer: SendProducer<String, ByteArray> = SendProducer(producerSettings, system)

            CoordinatedShutdown.get(system)
                .addTask(
                    CoordinatedShutdown.PhaseActorSystemTerminate(),
                    "close-sendProducer",
                    Supplier { sendProducer.close() }
                )

            return sendProducer
        }

        @JvmStatic
        private fun createProjectionFor(
            system: ActorSystem<*>,
            transactionManager: JpaTransactionManager,
            topic: String,
            sendProducer: SendProducer<String, ByteArray>,
            index: Int
        ): AtLeastOnceProjection<Offset, EventEnvelope<ShoppingCart.Event>> {

            val tag = ShoppingCart.TAGS[index]
            val sourceProvider: SourceProvider<Offset, EventEnvelope<ShoppingCart.Event>> =
                EventSourcedProvider.eventsByTag(system, JdbcReadJournal.Identifier(), tag)
            return JdbcProjection.atLeastOnceAsync(
                ProjectionId.of("PublishEventsProjection", tag),
                sourceProvider,
                { HibernateJdbcSession(transactionManager) },
                { PublishEventsProjectionHandler(topic, sendProducer) },
                system
            )
        }
    }
}
