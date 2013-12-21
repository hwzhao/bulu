package bulu.util

import akka.actor.ActorRef
import scala.collection.mutable.ArrayBuffer
import scala.xml.Node
import bulu.core.Field
import bulu.core.BitKey
import bulu.core.MeasureType
import bulu.core.CuboidPartition
import scala.collection.immutable.SortedMap


/*build*/
// ->master 
case object BuildStart
//master -> worker 
case class FetchDimension(cube: String, field: Field, sinker: ActorRef)
case class CubePartFinished(cube: String, condition: String,count:Int)
case class MemberList(cube: String, field: Field, list: List[Option[String]])
//worker -> sinker
case class RawData(cube: String, condition: String, sinker: ActorRef)
case class FetchRawData(cube: String, condition: String)
case class Record(rec: Map[String, Option[Any]])
case class RecordList(cube: String,rec: List[Map[String, Option[Any]]])
case class Cell(key: BitKey, rec: Map[String, Option[Any]])
case class DimensionFinshed(cube: String, partitionField: Field, list: List[Option[String]], workers: Seq[ActorRef])
case class CubeFinshed(cube: String,count:Int)
case class SaveCell(agg: (BitKey, Map[(String, MeasureType.MeasureType), BigDecimal]))
case class RawDataBegan(cube:String,sinker:ActorRef,partList:List[Option[String]],partitionField:Field,workerId:Int,workerCount:Int)
case object SendLeftRecords
case class RawDataFinished(cube:String,sinker:ActorRef,count:Int)
case class RawDataDispatchFinished(count:Int)
case object RecordFinished
case object CellSaved
case class SaveCellFinished(count:Int)
case object RequestAggs
case class CachedAggs(aggs:Map[BitKey, Map[(String, MeasureType.MeasureType), BigDecimal]])
//case class CachedCube(cubePart:Partition, aggs:Map[BitKey, Map[(String, MeasureType.MeasureType), BigDecimal]],workerId:Int, workerCount:Int)
/*build*/

/**query**/
case class Query(name: String, params: Map[String, String],workers:Seq[ActorRef])
case class CachePart(cube: String, partition: CuboidPartition, qeury: Query, cuboidSinker: ActorRef)
case class PartitionCached( query:Query, workerIndex:Int, workerCount:Int, partIndex:Int, partCount:Int )
case class CacheReply(query: Query, cacheSinker: ActorRef)
case class SendFinished(cube:String, query: CacheReply, workerIndex:Int, dispatchIndex:Int)
case class QueryReply(query: Query, reply: ActorRef,cuboidMask:BitKey, filterAndMatch:List[(BitKey, BitKey)])
case class QueryPart(cube: String, partition: CuboidPartition, query: Query, cuboidSinker: ActorRef, cuboidMask:BitKey, filterAndMatch:List[(BitKey, BitKey)])
case class QueryResult(query: Query, result:String)
//case class FetchedCell(cube:String, query: Query, key: BitKey, cell: Map[(String, MeasureType.MeasureType), BigDecimal], dispatchIndex:Int)
case class BaseCell(key: BitKey, cell: Map[(String, MeasureType.MeasureType), BigDecimal])
case class CacheCell(key: BitKey, cell:Map[(String, MeasureType.MeasureType), BigDecimal])
case class CacheFilterBegin(query: QueryReply)
case class HitCell(key: BitKey,values:Map[(String, MeasureType.MeasureType), BigDecimal])
case class QueryPartFinished(query: Query,  workerIndex:Int, dispatchIndex:Int, result: Map[BitKey, Map[(String, MeasureType.MeasureType), BigDecimal]])
case class FetchFinished(query: CacheReply)
case class BuildFinished(query: CacheReply)
case object CacheBegan
case object CacheCellFinished
case class CacheFinished(reply:ActorRef)
case class CachePartFinished(cube:String, count:Int, workerIndex:Int, dispatchIndex:Int)
case class AllCacheFinished(query: Query)
case class FilterBegan(query: Query)
case class FilterFinished(query: Query)
case object BaseFinished
case class QueryFinished(query: Query, nodes: Map[BitKey, Map[(String, MeasureType.MeasureType), BigDecimal]])
/**query**/
case class DimInstance(mask: BitKey, members: scala.collection.mutable.Map[String, BitKey])







//

