package bulu.actor.query

import akka.actor.Actor
import akka.actor.ActorLogging
import bulu.core.MeasureType
import scala.collection.mutable.ArrayBuffer
import bulu.core.BitKey
import bulu.util.CacheFilterBegin
import bulu.util.ConfigHelper
import akka.actor.Props
import bulu.util.QueryReply
import akka.routing.RoundRobinRouter
import bulu.util.CacheCell

class ParallelCache( cube : String, workerIndex : Int, dispatcherIndex : Int,
		aggs : ( List[BitKey], Map[( String, MeasureType.MeasureType ), ArrayBuffer[BigDecimal]] ) ) extends Actor with ActorLogging {

	def receive : Receive = {
		case CacheFilterBegin( queryReply  ) =>
			val keyList = aggs._1
			val valueList = aggs._2
			val cellAggregator = context.actorOf( Props( new CellAggregator( queryReply.query, workerIndex, dispatcherIndex, queryReply.reply, keyList.size ) ) ) //, name = "cellAggregator")
			val cellFilter = context.actorOf( Props( new CellFilter( cube, cellAggregator, queryReply.cuboidMask, queryReply.filterAndMatch ) ).
				withRouter( RoundRobinRouter( nrOfInstances = 8 ) ) ) //, name = "cellFilter")
			val fields = ConfigHelper.getMeasureFields( queryReply.query.name )

			for ( i <- 0 until keyList.size ) {
				val key = keyList( i )
				val values = for ( ( id, value ) <- valueList if fields.contains( id._1 ) ) yield ( id, value( i ) )
				cellFilter ! CacheCell( key, values.toMap )
			}
	}
}