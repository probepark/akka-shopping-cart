package shopping.cart

import javax.persistence.Entity
import javax.persistence.Id
import javax.persistence.Table
import javax.persistence.Version

@Entity
@Table(name = "item_popularity")
data class ItemPopularity(@Id val itemId: String = "", @Version val version: Long? = null, val count: Long = 0) {

    fun changeCount(delta: Long): ItemPopularity {
        return ItemPopularity(itemId, version, count + delta)
    }
}