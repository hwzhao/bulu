package bulu.actor.query

import akka.actor.ActorLogging
import akka.actor.Actor
import bulu.util.HitCell
import akka.actor.ActorRef
import bulu.core.MeasureType
import bulu.core.BitKey
import bulu.util.Query
import bulu.util.CacheFinished
import bulu.util.QueryPartFinished
import scala.collection.mutable.ArrayBuffer
import bulu.util.CacheBegan
import bulu.util.BaseCell
import bulu.util.CachePartFinished
import bulu.util.QueryReply
import akka.actor.Props
import akka.routing.RoundRobinRouter
import bulu.util.ConfigHelper
import bulu.util.CacheCell
import bulu.util.CacheCellFinished
import bulu.util.CacheFilterBegin

class DispatchCache(cube: String, workerIndex: Int, dispatcherIndex: Int) extends Actor with ActorLogging {
	var aggs:(List[BitKey], Map[(String, MeasureType.MeasureType), ArrayBuffer[BigDecimal]])=null
	val keyList = ArrayBuffer.empty[BitKey]
	val valueList = scala.collection.mutable.Map.empty[(String, MeasureType.MeasureType), ArrayBuffer[BigDecimal]]
	def hasAgg = !keyList.isEmpty
	def receive: Receive = {
		case CacheBegan =>
			keyList.clear
			valueList.clear
		//queryResult+=query->scala.collection.mutable.Map.empty[BitKey, Map[(String, MeasureType.MeasureType), BigDecimal]]
		case BaseCell(key, cell) =>
			//			log.debug("get a hit cell %s of partition (%s) of query (%s)".format(key, partition, query))
			keyList += key
			if (valueList.isEmpty) {
				for ((id, value) <- cell) {
					val values = ArrayBuffer.empty[BigDecimal]
					values += value
					valueList += id -> values
				}
			}
			else {
				for ((id, value) <- cell) {
					valueList(id) += value
				}
			}
		//queryResult(query) += key -> aggs(key)
		case CacheFinished(reply) =>
			log.info("cache is finished at cube (%s) workerIndex (%s) dispatcherIndex %s with agg (%s)".
				format(cube, workerIndex, dispatcherIndex, keyList.size))
				aggs=(keyList.toList,valueList.toMap)
			reply ! CachePartFinished(cube, keyList.size, workerIndex, dispatcherIndex)
		case queryReply: QueryReply =>
			log.info("began query %s at cube (%s) workerIndex (%s) dispatcherIndex %s with agg (%s)".
				format(queryReply.query, cube, workerIndex, dispatcherIndex, keyList.size))
			if (keyList.size == 0) {
				queryReply.reply ! QueryPartFinished(queryReply.query, workerIndex,dispatcherIndex, Map.empty[BitKey, Map[(String, MeasureType.MeasureType), BigDecimal]])
			}
			else {
//				val cacheFilter = context.actorOf( Props( new ParallelCache(cube, workerIndex, dispatcherIndex, aggs) ) ) //, name = "cellAggregator")
//				cacheFilter ! CacheFilterBegin( queryReply)
				val cellAggregator = context.actorOf(Props(new CellAggregator(queryReply.query, workerIndex, dispatcherIndex, queryReply.reply, keyList.size))) //, name = "cellAggregator")
				val cellFilter = context.actorOf(Props(new CellFilter(cube, cellAggregator, queryReply.cuboidMask, queryReply.filterAndMatch)).
					withRouter(RoundRobinRouter(nrOfInstances = 8))) //, name = "cellFilter")
				val fields = ConfigHelper.getMeasureFields(queryReply.query.name)

				for (i <- 0 until keyList.size) {
					val key = keyList(i)
					val values = for ((id, value) <- valueList if fields.contains(id._1)) yield (id, value(i))
					cellFilter ! CacheCell(key, values.toMap)
				}
			}
	}
}