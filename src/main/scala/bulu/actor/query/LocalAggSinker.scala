package bulu.actor.query

import akka.actor.ActorLogging
import akka.actor.Actor
import akka.actor.ActorRef
import bulu.util.CubePartFinished
import bulu.util.CellSaved
import bulu.util.QueryPartFinished
import scala.collection.immutable.SortedMap
import bulu.core.MeasureType
import bulu.core.BitKey

class LocalAggSinker(cube: String, count: Int, querySinker: ActorRef) extends Actor with ActorLogging {
	var left = count
	val result = scala.collection.mutable.Map.empty[BitKey, Map[(String, MeasureType.MeasureType), BigDecimal]]
	def receive: Receive = {
		case QueryPartFinished(query, workerIndex, dispatchIndex, aggs) =>
			log.info("end query %s at cube (%s) workerIndex (%s) dispatcherIndex %s with aggs (%s)".
				format(query, cube, workerIndex, dispatchIndex, aggs.size))
			left -= 1
			if (!aggs.isEmpty) {
				if (result.isEmpty) {
					result ++= aggs
				}
				else {
					for ((id, values) <- aggs) {
						result.get(id) match {
							case None => result += (id -> values)
							case Some(cell) => result += (id -> aggValue(cell, values))
						}
					}
				}
			}
			if (left == 0) {
				log.info("end worker (%s) of query (%s) with list (%s)".
					format(workerIndex, query, result.size))
				querySinker ! QueryPartFinished(query, workerIndex, Int.MaxValue, result.toMap)

			}
	}

	def aggValue(cell: Map[(String, MeasureType.MeasureType), BigDecimal],
		values: Map[(String, MeasureType.MeasureType), BigDecimal]): Map[(String, MeasureType.MeasureType), BigDecimal] = {
		val agg = scala.collection.mutable.Map.empty[(String, MeasureType.MeasureType), BigDecimal]
		for ((id, value) <- cell) {
			agg += id -> (cell(id) + values(id))
		}
		agg.toMap
	}
}