package bulu.actor.query

import akka.routing.RoundRobinRouter
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorLogging
import akka.actor.Props
import bulu.util._
import bulu.core.MeasureType 
import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.hbase.util.Bytes
import bulu.core.BitKey
import bulu.core.CuboidPartition

class QueryDispatcher(partition: CuboidPartition, localIndex: Int, localCount: Int) extends Actor with ActorLogging {
	val dispatchCache = context.actorOf(Props(new DispatchCache(partition.cube, partition.partIndex, localIndex)), name = "dispatchCache")
	val hbaseFetcher = context.actorOf(Props(new HbaseFetcher(partition, localIndex, localCount,dispatchCache )), name = "hbaseFetcher")

//	var aggs = scala.collection.mutable.Map.empty[BitKey, Map[(String, MeasureType.MeasureType), BigDecimal]] //TODO: here just handle the base cuboid, not handle middle cuboids
	
//	var reply: ActorRef = null
//	val aggSender = {
//		val result = scala.collection.mutable.Set.empty[(Int, Int)]
//		for (i <- 0 until partition.partCount; j <- 0 until localCount) {
//			result += ((i, j))
//		}
//		result
//	}
	def receive = {

		case cacheReply: CacheReply =>
				hbaseFetcher ! cacheReply

//		case FetchFinished(query) =>
//			if (aggSender.isEmpty) {
//				self ! BuildFinished(query)
//			}
//		case SendFinished(cube, query, workerIndex, dispatchIndex) =>
//			aggSender -= ((workerIndex, dispatchIndex))
//			if (aggSender.isEmpty) {
//				log.info("cached query (%s) of partition (%s)".format(query, partition))
//				if (!isReady(partition.cuboid)) {
//					//TODO create middle cuboid
//				}
//
//				self ! BuildFinished(query)
//			}
//		case BuildFinished(query) =>
//			query.reply ! PartitionCached(query.query, partition.partIndex, partition.partCount,
//				localIndex, localCount)
		case query: QueryReply =>
			//			log.info("get query (%s)  partition (%s)".format(query, partition))
//			reply = query.reply
			//			if ( queryResult.contains( query.query ) ) {
			//				reply ! QueryPartFinished( query.query, partition, queryResult( query.query ).toMap )
			//			}
			//			else {
			//				queryResult += query.query -> scala.collection.mutable.Map.empty[BitKey, Map[( String, MeasureType.MeasureType ), BigDecimal]]
			//			}
			dispatchCache ! query  
			
//			log.info("execute query (%s)  partition (%s) @ %s of %s ".
//				format(query, partition, localIndex, localCount))
//			
//			val cellAggregator = context.actorOf(Props(new CellAggregator(query.query,(partition, localIndex, localCount), query.reply)))//, name = "cellAggregator")
//			val cellFilter = context.actorOf(Props(new CellFilter(partition.cube, cellAggregator)).withRouter(RoundRobinRouter(nrOfInstances = 64)))//, name = "cellFilter")
//			cellFilter ! CacheBegan(query.query)
//			val maskAndFilterList = getMaskAndFilters(partition, query.query)
//			for (agg <- aggs) {
//				cellFilter ! CacheCell(query.query, agg._1, maskAndFilterList)
//			}
//			Thread.sleep(100)
//			//queryResult += query.query -> scala.collection.mutable.Map.empty[BitKey, Map[(String, MeasureType.MeasureType), BigDecimal]]
//			cellFilter ! CacheFinished(query.query)
	}
	

	def isReady(cuboid: BitKey) = true //use base cuboid only
}