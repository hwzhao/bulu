package bulu.actor.query

import akka.actor.ActorLogging
import akka.actor.Actor
import bulu.util.HitCell
import akka.actor.ActorRef
import bulu.core.MeasureType
import bulu.core.BitKey
import bulu.util.Query
import bulu.util.FilterFinished
import bulu.util.QueryPartFinished
import bulu.core.CuboidPartition
import bulu.util.CacheCellFinished
import scala.collection.immutable.SortedMap


class CellAggregator(query:Query,workerIndex: Int, dispatchIndex:Int,reply:ActorRef, count:Int)  extends Actor with ActorLogging {
	val aggs = scala.collection.mutable.Map.empty[BitKey, Map[(String, MeasureType.MeasureType), BigDecimal]]
	var left=count
	def receive: Receive = {
		case CacheCellFinished  =>
			left-=1
			if(left==0){
				log.info("filter is finished at query (%s) on worker %s local %s result(%s) in (%s)".
				format(query, workerIndex, dispatchIndex, aggs.size, count))
				reply ! QueryPartFinished(query, workerIndex,dispatchIndex, aggs.toMap)
			}
		case HitCell( key,values) =>
//			log.debug("get a hit cell %s of partition (%s) of query (%s)".format(key, partition, query))
			aggs.get(key) match {
				case None => aggs += (key -> values)
				case Some(cell) => aggs += (key -> aggValue(cell,values))
			}
	}
	def aggValue(cell: Map[(String, MeasureType.MeasureType), BigDecimal],
			values: Map[(String, MeasureType.MeasureType), BigDecimal]): Map[(String, MeasureType.MeasureType), BigDecimal] = {
		val agg=scala.collection.mutable.Map.empty[(String, MeasureType.MeasureType), BigDecimal]
		for((id, value)<-cell){
			agg+= id->(cell(id)+values(id))
		}
		agg.toMap
	}

}