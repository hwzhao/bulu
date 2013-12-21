package bulu.actor.query
import akka.actor.Actor
import akka.actor.ActorLogging
import bulu.util._
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.util.Bytes
import akka.actor.actorRef2Scala
import scala.Array.canBuildFrom
import akka.actor.ActorRef
import bulu.core.BitKey
class CellFilter(cube:String, aggregator:ActorRef,cuboidMask:BitKey, filterAndMatch:List[(BitKey, BitKey)]) extends Actor with ActorLogging {

	var queryMsg: Query = null
	def receive: Receive = {
		
		case CacheCell(key, values) =>
			val cuboidKey=key.and(cuboidMask)
			for(maskAndFilter <- filterAndMatch){
				val result = cuboidKey and maskAndFilter._1
				if ( result == maskAndFilter._2 ){
					aggregator ! HitCell(cuboidKey,values)
				}
				
			}
			aggregator ! CacheCellFinished
			
//		case CacheFinished(query) =>
//			aggregator ! FilterFinished(query)
//		case FetchHitCell(query: Query, cuboid: Array[Byte], key: BitKey) =>
//			queryMsg = query
//			val table = new HBase(cuboid)
//			val queryInstance = Config.getCubeQery(queryMsg.cube, queryMsg.name)
//			val columns = queryInstance.get("cell").get.map {
//				measure =>
//					val m = measure.asInstanceOf[(String, String)]
//					Convert.measureName2Qualifier((m._1, m._2))
//			} ++ queryInstance.get("row").get.asInstanceOf[List[String]].map(Bytes.toBytes(_))
//
//			table.get(key, columns, sendHit _)
//			table.close
//		case FetchCell(query, cuboid, start, end, filters) =>
//			queryMsg = query
//			val table = new HBase(cuboid)
//			val queryInstance = Config.getCubeQery(queryMsg.cube, queryMsg.name)
//			val columns = queryInstance.get("cell").get.map {
//				measure =>
//					val m = measure.asInstanceOf[(String, String)]
//					Convert.measureName2Qualifier((m._1, m._2))
//			} ++ queryInstance.get("row").get.asInstanceOf[List[String]].map(Bytes.toBytes(_))
//			table.scan(columns, start, end, filters, filter _)
//
//			table.close
	}
//	def sendHit(key: BitKey, keyValues: Array[KeyValue]) = {
//		if (keyValues.size > 0) {
//			log.info("got [%s]->[%s] from hbase".format( key,keyValues.map(
//					f=>Convert.qualifier2MeasureName(f.getQualifier())+Bytes.toLong(f.getValue()).toString)))
//			val cell = for {
//				keyValue <- keyValues
//				aggKey = Convert.qualifier2MeasureName(keyValue.getQualifier)
//				value: Long = Bytes.toLong(keyValue.getValue)
//
//			} yield (aggKey, value)
//			
//			sender ! HitCell(queryMsg, key, cell.toMap)
//		}
//	}
//	def filter(row: Array[Byte], keyValues: Array[KeyValue], filters: List[BitKey]) {
//		val bitKey: BitKey = Convert.fromByteArray(row)
//		if (hit(bitKey, filters)) {
//			sendHit(bitKey, keyValues)
//		}
//	}
//	def hit(key: BitKey, filters: List[BitKey]): Boolean = {
//		for (filter <- filters)
//			if (key.equals(key and filter)) { return true }
//		return false
//	}
}