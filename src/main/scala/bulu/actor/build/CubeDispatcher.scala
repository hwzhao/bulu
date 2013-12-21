package bulu.actor.build

import akka.routing.RoundRobinRouter
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorLogging
import akka.actor.Props
import bulu.util._
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.util.Bytes
import bulu.core.BitKey


class CubeDispatcher(cube: String, reply: ActorRef,
		dimVK: Map[String, Map[Option[String], BitKey]],
			workerId:Int,id:Int) extends Actor with ActorLogging {
	val partition = "cube dispatchter %s of worker %s".format(id,workerId)
	val aggregator = context.actorOf(Props(new Aggregator(cube,partition,this,reply)))//, name = "aggregator")
	val keyConvertor = context.actorOf(Props(new KeyConvertor(cube, dimVK,aggregator)).withRouter(RoundRobinRouter(nrOfInstances = 8)))//, name = "keyConvertor")

	def receive = {
		//		case fetch: FetchRawData =>
		//			log.info("begin fetch rawdata for cube (%s) with condition (%s)".format(fetch.cube,fetch.condition))
		//			partition=fetch.condition
		//			fetcher ! fetch

//		case rb:RawDataBegan =>
//			partition=Partition(rb.cube,)
		case record: Record =>
			
			log.debug("get a record for cube (%s) with condition (%s): (%s)".format(cube, partition, record))
			keyConvertor ! record
		case rddf: RawDataDispatchFinished =>
			
			log.info("end fetch rawdata for cube (%s) with condition (%s)".format(cube, partition))
			Thread.sleep(100)
			aggregator ! rddf
		case message:Any => throw new Exception("unsupported message: "+message)
//		case cell: Cell =>
//			log.debug("get a cell for cube (%s) with condition (%s) : (%s : %s)".format(cube, partition, cell.key, cell.rec))
//			cellCount+=1
//			aggregator ! cell
//		case RecordFinished =>
//			log.info("end convert record %s for cube (%s) with condition (%s)".format(cellCount, cube, partition))
//			Thread.sleep(100)
//			aggregator ! RecordFinished

	}

}