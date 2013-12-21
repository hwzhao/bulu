package bulu.actor.query
import scala.collection.JavaConversions._
import akka.actor.Actor
import bulu.util._
import akka.actor.ActorLogging
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.KeyValue
import scala.collection.mutable.ArrayBuffer
import akka.actor.actorRef2Scala
import scala.Array.canBuildFrom
import bulu.core.BitKey
import bulu.core.CuboidPartition
import akka.actor.ActorRef

class HbaseFetcher(partition: CuboidPartition,localIndex:Int, localCount:Int,cache:ActorRef) extends Actor with ActorLogging {
	//	val dims = scala.collection.mutable.Map.empty[String, Map[BitKey, Option[String]]]
	//	var maxKeyTrue, maxKeyFalse : BitKey = null
	//	val ParameterExp = """\[([\S\s]+)\]""".r

//	lazy val theMask = getMaskAndFilter(partition)
//	lazy val theStart =SystemHelper.getStartBit(theMask)
//	lazy val theLength = SystemHelper.getLength(theMask, theStart)
//	def getMaskAndFilter(partition: CuboidPartition) = {
//		val masks = scala.collection.mutable.Map.empty[String, BitKey]
//		def saveMasks(row: Array[Byte], family: Array[Byte], qualifier: Array[Byte], value: Array[Byte]) {
//			masks += (Bytes.toString(qualifier) -> Convert.fromByteArray(value))
//		}
//		val maskTable = new HBase(HBase.getTableName(partition.cube, TableCategory.Mask));
//		maskTable.scan(saveMasks _)
//		
//		maskTable.close
//		val mask = masks(partition.dim4partition)
//		log.info("the mask is " + mask)
		
		//val startBit =SystemHelper. getStartBit(mask)
//		val bin = partition.partCount.toBinaryString
//		val theFix = ConfigHelper.sharding(partition.partCount)(partition.partIndex)
//		val theMask = BitKey.Factory.makeBitKey(mask.cardinality(), false)
//		val theFilter = BitKey.Factory.makeBitKey(mask.cardinality(), false)
//		for (i <- 0 until theFix._1.length) {
//			if (theFix._1(i) == '1') {
//				theMask.set(i + startBit)
//			}
//			if (theFix._2(i) == '1') {
//				theFilter.set(i + startBit)
//			}
//		}
//		log.info("partition (%s) has the Mask (%s) and filter (%s)".format(partition, theMask, theFilter))
//		(theMask, theFilter)
//		mask
//	}

	
	def receive: Receive = {
		case query: CacheReply =>
			
//			def send(key: BitKey, keyValues: Array[KeyValue], workerIndex:Int, dispatchIndex:Int) = {
//				if (keyValues.size > 0) {
//					//			log.info( "got [%s]->[%s] from hbase".format( key, keyValues.map(
//					//				f => Convert.qualifier2MeasureName( f.getQualifier() ) + Bytes.toLong( f.getValue() ).toString ) ) )
//					val cell = for {
//						keyValue <- keyValues
//						aggKey = Convert.qualifier2MeasureName(keyValue.getQualifier)
//						value: scala.math.BigDecimal = BigDecimal(Bytes.toBigDecimal(keyValue.getValue))
//
//					} yield (aggKey, value)
//					if(workerIndex==partition.partIndex && dispatchIndex==this.localIndex)
//						{
//							sender ! BaseCell(key, cell.toMap)
//							selfCount+=1						
//						}
//						
//					else{
//						val worker=query.query.workers(workerIndex)
//						
//						worker ! FetchedCell(partition.cube, query.query, key, cell.toMap, dispatchIndex)
//						if(otherCounts.contains(workerIndex, dispatchIndex))
//						{
//							otherCounts((workerIndex, dispatchIndex))+=1
//						}
//						else{
//							otherCounts +=(workerIndex, dispatchIndex)->1		
//						}
//					}
//				}
//			}
			def filter(row: Array[Byte], keyValues: Array[KeyValue]) {
				val bitKey: BitKey = Convert.fromByteArray(row)
				val cell = for {
						keyValue <- keyValues
						aggKey = Convert.qualifier2MeasureName(keyValue.getQualifier)
						value: scala.math.BigDecimal = BigDecimal(Bytes.toBigDecimal(keyValue.getValue))

					} yield (aggKey, value)
				cache  ! BaseCell(bitKey, cell.toMap)
//				val bin=SystemHelper.getBinaryString(bitKey, theStart, theLength)
//				val key=Integer.parseInt(bin,2)
//				val workerIndex=partition.partIndex//key % partition.partCount
//				val dispatchIndex=key % localCount//(key/partition.partCount).toInt % localCount
				
//				log.debug("send a bitkey(%s) key(%s) fetched cell to worker (%s) dispatcher(%s)".format(bitKey,key, workerIndex, dispatchIndex))
//				send(bitKey, keyValues)//,workerIndex,dispatchIndex)
				
			}
			cache ! CacheBegan
			val baseCuboid = ConfigHelper.getBaseCuboidName(partition.cube)
			val table = new HBase(Bytes.toBytes(baseCuboid))
			
			val regions=table.localRegions
			log.info("cache regionCount=%s".format(regions.size))
			for(i<-0 until regions.size){
				log.debug("i=%s localIndex=%s localCount=%s".format(i,localIndex, localCount))
				if(i%localCount==localIndex){
					val startEndKey=regions(i)
					log.info("Hbase begin fetch: Actor (%s) @ %s of %s handle the region (%s,%s) @ %s of @s".
							format(self, localIndex, localCount,Convert.fromByteArray(startEndKey._1), Convert.fromByteArray(startEndKey._2), i, regions))
					val count=table.scan(startEndKey._1, startEndKey._2, filter _)
					log.info("Hbase end fetch(%s): Actor (%s) @ %s of %s handle the region (%s,%s) @ %s of @s".
							format(count,self, localIndex, localCount, Convert.fromByteArray(startEndKey._1),Convert.fromByteArray(startEndKey._2), i, regions))
					 
				}
			}
			table.close
			cache ! CacheFinished(query.cacheSinker)
//			for(worker<-query.query.workers){
//				worker ! SendFinished(partition.cube, query, partition.partIndex, localIndex)
//			}
	}
}