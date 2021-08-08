package shopping.cart

import akka.projection.eventsourced.EventEnvelope
import akka.projection.jdbc.javadsl.JdbcHandler
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import shopping.cart.ShoppingCart.ItemAdded
import shopping.cart.repository.HibernateJdbcSession
import shopping.cart.repository.ItemPopularityRepository

class ItemPopularityProjectionHandler(val tag: String, private val itemPopularityRepository: ItemPopularityRepository) :
        JdbcHandler<EventEnvelope<ShoppingCart.Event>, HibernateJdbcSession>() {

    private val logger: Logger = LoggerFactory.getLogger(javaClass)

    override fun process(session: HibernateJdbcSession, envelope: EventEnvelope<ShoppingCart.Event>) {

        val event = envelope.event()

        if (event is ItemAdded) {
            val (_, itemId, quantity) = event
            val existingItemPop = findOrNew(itemId)
            val updatedItemPop = existingItemPop.changeCount(quantity.toLong())
            itemPopularityRepository.save(updatedItemPop)
            logger.info(
                    "ItemPopularityProjectionHandler({}) item popularity for '{}': [{}]",
                    tag,
                    itemId,
                    updatedItemPop.count)
        } else {
            // skip all other events, such as `CheckedOut`
        }
    }

    private fun findOrNew(itemId: String): ItemPopularity {
        return itemPopularityRepository.findById(itemId).orElseGet { ItemPopularity(itemId, 0, 0) }
    }
}