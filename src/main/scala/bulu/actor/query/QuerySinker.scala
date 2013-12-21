package bulu.actor.query
import akka.actor.Actor
import bulu.util._
import akka.actor.ActorLogging
import org.apache.hadoop.hbase.util.Bytes
import java.sql.DriverManager
import java.sql.ResultSet
import akka.actor.actorRef2Scala
import akka.actor.ActorRef
import bulu.core.{Field,MeasureType}
import scala.collection.mutable.ArrayBuffer
import bulu.core.BitKey




class QuerySinker( count : Int, reply : ActorRef ) extends Actor with ActorLogging {
	var left = count
	val result=scala.collection.mutable.Map.empty[BitKey, Map[( String, MeasureType.MeasureType ), BigDecimal]]
	
	
	def receive: Receive = {
		case QueryPartFinished( query,workerIndex, dispatchIndex, aggs) =>
			log.info("end query (%s)  workerIndex (%s) with aggs (%s)".
				format(query, workerIndex, aggs.size))
			left -= 1
			if (result.isEmpty) {
				result ++= aggs
			}
			else {
				for ((id,values)<-aggs){
					result.get(id) match {
						case None => result += (id -> values)
						case Some(cell) => result += (id -> aggValue(cell,values))
					}
				}
			}
			if (left == 0) {
				
				log.info("end all query (%s) parts" format query)
				reply ! QueryFinished(query,result.toMap)
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