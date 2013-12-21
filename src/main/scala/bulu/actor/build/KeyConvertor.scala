package bulu.actor.build
import akka.actor.Actor
import bulu.util._
import akka.actor.ActorLogging
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.KeyValue
import akka.actor.actorRef2Scala
import scala.Array.canBuildFrom
import bulu.core.BitKey
import akka.actor.ActorRef

class KeyConvertor(cube: String, dims: Map[String, Map[Option[String], BitKey]],aggregator:ActorRef) extends Actor with ActorLogging {

	def receive: Receive = {
		case Record(rec) =>

			val node = handleDimensionMember(cube, rec)
			aggregator ! Cell(node, removeDimensions(rec))

		case raw: RawDataFinished =>
			aggregator ! RecordFinished

	}

	def getToBytes(dataType: String) = {

		dataType match {
			case "String" => Bytes.toBytes(_: String)
			case "Int" => Bytes.toBytes(_: Int)
			case _: String => Array(_: Any)
		}
	}

	def handleDimensionMember(cube: String, rec: Map[String, Option[Any]]): BitKey = {
		var allkey: BitKey = BitKey.EMPTY

		//		val dimVKTable = new HBase(HBase.getTableName(cube, TableCategory.DimVK))

		for (d <- ConfigHelper.getDimensionNames(cube)) {

			val key = dims.get(d).get.get(getValue(rec.get(d).get.toString)).get //dimVKTable.get(Bytes.toBytes(d), Bytes.toBytes(HBase.DefaultFamily), Bytes.toBytes(rec.get(d).toString));
			assert(key != null)
			allkey = allkey.or(key) //Convert.fromByteArray(key))

		}

		//		dimVKTable.close
		//		log.debug("allkey is " + allkey)
		allkey
	}

	val someExp = """Some\(([\S\s]+)\)""".r
	def getValue(str: String): Option[String] = str match {
		case "None" => None
		case "Some()" => Some("")
		case someExp(x) => Some(x)

	}
	val measures = ConfigHelper.getMeasureNames(cube)
	def removeDimensions(rec: Map[String, Option[Any]]) = {

		val result = rec.filter(kv => measures.contains(kv._1))
		//		log.debug("rec for measure:" + result)
		result
	}
}