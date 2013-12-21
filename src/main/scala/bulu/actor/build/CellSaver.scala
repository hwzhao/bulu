package bulu.actor.build
import akka.actor.Actor
import bulu.util._
import akka.actor.ActorLogging
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.hbase.client.Put
import akka.actor.ActorRef

class CellSaver(cube: String, sinker:ActorRef) extends Actor with ActorLogging {
	var table: HBase = null
	var count=0
	override def preStart(): Unit = {
		table = new HBase(HBase.getTableName(cube, TableCategory.CuboidBase));
	}
	override def postStop(): Unit = {
			table.close
	}
	def receive: Receive = {
		case SaveCell(agg) =>
			count+=1
			val maps = for (mea <- agg._2) yield (Convert.measureName2Qualifier(mea._1), 
					HBase.bigDecimal2Bytes(new java.math.BigDecimal(mea._2.toDouble)))
		
			try{
			table.put(Convert.toByteArray(agg._1), Bytes.toBytes(HBase.DefaultFamily), maps)
			}catch{
			case e:Exception=> log.error("put exception ",e)
			}
			sinker ! CellSaved
//		case CellFinished =>
//			table.close
//			sender ! SaveCellFinished(count)

	}

}