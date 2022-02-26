package shopping.cart

import akka.actor.typed.ActorSystem
import akka.cluster.sharding.typed.ShardedDaemonProcessSettings
import akka.cluster.sharding.typed.javadsl.ShardedDaemonProcess
import akka.persistence.jdbc.query.javadsl.JdbcReadJournal
import akka.persistence.query.Offset
import akka.projection.ProjectionBehavior
import akka.projection.ProjectionId
import akka.projection.eventsourced.EventEnvelope
import akka.projection.eventsourced.javadsl.EventSourcedProvider
import akka.projection.javadsl.ExactlyOnceProjection
import akka.projection.javadsl.SourceProvider
import akka.projection.jdbc.javadsl.JdbcProjection
import org.springframework.orm.jpa.JpaTransactionManager
import shopping.cart.repository.HibernateJdbcSession
import shopping.cart.repository.ItemPopularityRepository
import java.util.Optional

sealed class ItemPopularityProjection {

    companion object {

        @JvmStatic
        fun init(
            system: ActorSystem<*>,
            transactionManager: JpaTransactionManager,
            repository: ItemPopularityRepository
        ) {

            ShardedDaemonProcess.get(system)
                .init(
                    ProjectionBehavior.Command::class.java,
                    "ItemPopularityProjection",
                    ShoppingCart.TAGS.size,
                    { index ->
                        ProjectionBehavior.create(
                            createProjectionFor(system, transactionManager, repository, index)
                        )
                    },
                    ShardedDaemonProcessSettings.create(system),
                    Optional.of(ProjectionBehavior.stopMessage())
                )
        }

        @JvmStatic
        private fun createProjectionFor(
            system: ActorSystem<*>,
            transactionManager: JpaTransactionManager,
            repository: ItemPopularityRepository,
            index: Int
        ):
            ExactlyOnceProjection<Offset, EventEnvelope<ShoppingCart.Event>> {

            val tag = ShoppingCart.TAGS[index]
            val sourceProvider: SourceProvider<Offset, EventEnvelope<ShoppingCart.Event>> =
                EventSourcedProvider.eventsByTag(system, JdbcReadJournal.Identifier(), tag)

            return JdbcProjection.exactlyOnce(
                ProjectionId.of("ItemPopularityProjection", tag),
                sourceProvider,
                { HibernateJdbcSession(transactionManager) },
                { ItemPopularityProjectionHandler(tag, repository) },
                system
            )
        }
    }
}
