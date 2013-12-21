package bulu.actor.build
import akka.actor.Actor
import bulu.util._
import akka.actor.ActorLogging
import scala.math.BigDecimal
import akka.actor.actorRef2Scala
import scala.math.BigDecimal.int2bigDecimal
import bulu.core.MeasureType
import bulu.core.BitKey
import akka.actor.Props
import akka.routing.RoundRobinRouter
import akka.actor.ActorRef
/**
 * Random record Aggregator
 */
class Aggregator(cube: String, partition: String, dispatcher: CubeDispatcher, reply: ActorRef) extends Actor with ActorLogging {
	var localSinker: ActorRef = null //context.acotrOf(Props(new DispatchSinker(cube,partition,reply)))
	var cellSaver: ActorRef = null // context.actorOf(Props(new CellSaver(cube)))//.withRouter(RoundRobinRouter(nrOfInstances = 200)))//, name = "cellSaver")
	//	val nodeKeys = Agent(Map.empty[BitKey, Int])(context.system)
	val aggs = scala.collection.mutable.Map.empty[BitKey, Map[(String, MeasureType.MeasureType), BigDecimal]]
	def getAggs = aggs
	var recordCount = 0
	var expectedRecordCount=Int.MaxValue
	def isRecordFinished = recordCount == expectedRecordCount
	def receive: Receive = {
		case Cell(key, values) =>
			recordCount += 1
			aggs.get(key) match {
				case None =>
					val all = for (m <- ConfigHelper.getBuildMeasures(cube))
						yield m -> getValue(m._2, m._1, values)
					aggs += (key -> all.toMap)
				case Some(cell) =>
					val all = for (m <- ConfigHelper.getBuildMeasures(cube))
						yield m -> aggValue(m._2, m._1, values, cell)
					aggs += (key -> all.toMap)

			}
			
				if (isRecordFinished) self ! RawDataDispatchFinished(expectedRecordCount)
		case RawDataDispatchFinished( count) =>
			expectedRecordCount = count
			if (isRecordFinished) {

				log.info("begin to save %s count  cells of %s  @ %s".format(aggs.size, cube, partition))
				if (aggs.size == 0) {
					log.info("end to save aggregating cells %s count for cube (%s) with condition (%s)".format(0, cube, partition))
					reply ! CubePartFinished(cube, partition, 0)
				}
				else {
					localSinker = context.actorOf(Props(new DispatchSinker(cube, partition, aggs.size, reply)))
					cellSaver = context.actorOf(Props(new CellSaver(cube, localSinker)).withRouter(RoundRobinRouter(nrOfInstances = 8))) //, name = "cellSaver")
					for (agg <- aggs) {
						cellSaver ! SaveCell(agg)
					}

					aggs.clear
				}
			}
		//			Thread.sleep(50)
		//			log.info("end to send %s count saving cells of  %s basecuboid".format(aggs.size, cube))
		//			cellSaver ! CellFinished

		//		case sf: SaveCellFinished =>
		//			log.info("end to save aggregating cells %s count for cube (%s) with condition (%s)".format(sf.count, cube, partition))
		//			reply ! CubePartFinished(cube, partition, sf.count)

	}
	//	override def postStop = {
	//		if (table != null) table.close
	//	}

	def aggValue(aggType: MeasureType.MeasureType, field: String,
		rec: Map[String, Option[Any]], agg: Map[(String, MeasureType.MeasureType), BigDecimal]): BigDecimal = {
		agg.get((field, aggType)) match {
			case None => getValue(aggType, field, rec)
			case Some(value) => getValue(aggType, field, rec) + (value)
		}
	}
	def getValue(aggType: MeasureType.MeasureType, field: String, rec: Map[String, Option[Any]]): BigDecimal = {
		aggType match {
			case MeasureType.Count => 1
			case _ => rec.get(field).get match {
				case None => 0
				case Some(x) => HBase.toBigDecimal(x)
			}

		}
	}
}