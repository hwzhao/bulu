package bulu.actor

import akka.actor.Actor
import akka.actor.Props
import akka.routing.RoundRobinRouter
import akka.actor.ActorRef
import bulu.util._
import akka.actor.ActorLogging
import bulu.actor.query.HbaseFetcher
import bulu.actor.query.QuerySinker
import bulu.core.MeasureType
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.KeyValue
import java.text.DecimalFormat
import bulu.core.BitKey
import bulu.actor.query.CacheSinker
import bulu.core.CuboidPartition
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

class Executer(cube: String, workers: Seq[ActorRef], report: ActorRef) extends Actor with ActorLogging {

  val allMask = {
    val masks = scala.collection.mutable.Map.empty[String, BitKey]
    def saveMasks(row: Array[Byte], family: Array[Byte], qualifier: Array[Byte], value: Array[Byte]) {
      masks += (Bytes.toString(qualifier) -> Convert.fromByteArray(value))
    }

    val maskTable = new HBase(HBase.getTableName(cube, TableCategory.Mask));
    maskTable.scan(saveMasks _)
    maskTable.close
    masks.toMap

  }
  val allDimKV = scala.collection.mutable.Map.empty[String, Map[String, Map[BitKey, Option[String]]]]
  val workParts = scala.collection.mutable.Map.empty[Int, ActorRef]
  //	val replyto = self
  val querySinkers = scala.collection.mutable.Map.empty[Query, ActorRef]
  var cacheSinker: ActorRef = null
  def receive: Receive = {
    case query: Query =>
      log.info("begin execute query [%s] ...".format(query))
      if (cacheSinker != null)
        self ! AllCacheFinished(query)
      else {
        //TODO add query parsers and find the cubes and execute plan
        //Now just the hard code and just query a cube. not support cubes join
        val count = ConfigHelper.actorsForWorker * workers.size
        cacheSinker = context.actorOf(Props(new CacheSinker(query, count, self)), name = "cacheSinker" + cube + workers.size.toString)
        //			val cube = ConfigHelper.getQueryCube( query.name )
        for (i <- 0 until workers.size) {
          workParts += i -> workers(i)
          val partition = CuboidPartition(cube, i, workers.size)
          workers(i) ! CachePart(cube, partition, query, cacheSinker)
        }
      }
    case AllCacheFinished(query) =>
      log.info("end cache query [%s] ...".format(query))

      val count = workers.size //local join, so here just workers count
      val querySinker = context.actorOf(Props(new QuerySinker(count, self)))//, name = "querySinker" + cube + workers.size.toString)
      querySinkers += query -> querySinker
      //			val cube = ConfigHelper.getQueryCube( query.name )
      val cuboidMask = getCuboidMask(cube, query.name)
      val filterAndMatch = getFilterMaskAndMatch(cube, query)
      for (i <- 0 until workers.size) {
        workParts += i -> workers(i)
        val partition = CuboidPartition(cube, i, workers.size)
        workers(i) ! QueryPart(cube, partition, query, querySinker, cuboidMask, filterAndMatch)
      }
    case QueryFinished(query, aggs) =>
      //transform
      log.info("end execute query [%s] ...".format(query))
      //			context.stop(cacheSinker) cache is alive always
      querySinkers.remove(query) match {
        case None =>
        case Some(act) => context.stop(act)
      }

      val result = format(query, aggs)
      if (ConfigHelper.getIsWriteDisk(query.name)) {
        val filename = query.name // + "_" + query.params
        ToolHelper.writeToFile(filename, result)
        log.info("write to file (%s) with aggs(%s)".format(filename, aggs.size))
      } else {
        println(result)

      }
    //			report ! QueryResult(query, )

  }
  val ParameterExp = """\[([\S\s]+)\]""".r
  def getFilterMaskAndMatch(cube: String, query: Query): List[(BitKey, BitKey)] = {
    val vkTable = new HBase(Bytes.toBytes(cube + "-" + TableCategory.DimVK))
    def getKey(col: String, member: String): BitKey = {
      val memberValue = member match {
        case "None" => "None"
        case "" => "None"
        case _ => "Some(%s)" format member
      }
      val bytesKey = vkTable.get(col, memberValue)
      Convert.fromByteArray(bytesKey)
    }
    val maskTable = new HBase(Bytes.toBytes(cube + "-" + TableCategory.Mask))

    def getMask(col: String) = {
      val bytesKey = maskTable.get(HBase.DefaultMaskRowId, col)
      Convert.fromByteArray(bytesKey)
    }
    val result = ArrayBuffer.empty[(BitKey, BitKey)]
    val orCount = ConfigHelper.getConfig.getInt(query.name + ".where.count")
    for (i <- 1 to orCount) {
      val andCount = ConfigHelper.getConfig.getInt(query.name + ".where.filter" + i + ".count")
      var theMask = BitKey.Factory.makeBitKey(1, false)
      var theFilter = BitKey.Factory.makeBitKey(1, false)
      for (j <- 1 to andCount) {
        val andCondition = ConfigHelper.getConfig.getStringList(query.name + ".where.filter" + i + ".condition" + j)
        val memberValue = andCondition.get(2) match {
          case ParameterExp(name) =>
            query.params.get(name).get
          case s: String => s
          case _ => throw new Exception
        }
        val key = getKey(andCondition.get(1), memberValue)
        theFilter = theFilter or key
        val mask = getMask(andCondition.get(1))
        theMask = theMask or mask

      }
      result += Tuple2(theMask, theFilter)
    }
    vkTable.close
    maskTable.close
    result.toList
  }
  /**
   * for aggregate the base cuboid
   */
  def getCuboidMask(cube: String, queryName: String): BitKey = {
    val maskTable = new HBase(Bytes.toBytes(cube + "-" + TableCategory.Mask))
    def getMask(col: String) = {
      val bytesKey = maskTable.get(HBase.DefaultMaskRowId, col)
      Convert.fromByteArray(bytesKey)
    }
    var theMask = BitKey.Factory.makeBitKey(1, false)
    val dims = ConfigHelper.getQueryCuboid(queryName)

    for (name <- dims) {
      theMask = theMask.or(getMask(name))
    }

    maskTable.close
    theMask

  }
  /**
   * for identify the cuboid
   */
  def getCuboidKey(cube: String, queryName: String): BitKey = {

    val dims = ConfigHelper.getQueryCuboid(queryName)
    val allDims = ConfigHelper.getDimensionNames(cube)
    val bitKey = BitKey.Factory.makeBitKey(allDims.size, false)
    for (i <- 0 until allDims.size) {
      if (dims.contains(allDims(i))) {
        bitKey.set(i)
      }
    }
    bitKey

  }
  def format(query: Query, aggs: Map[BitKey, Map[(String, MeasureType.MeasureType), BigDecimal]]): String = {
    import scala.collection.JavaConversions._
    log.info("begin render query (%s) with result count (%s)".format(query, aggs.size))
    val cube = ConfigHelper.getQueryCube(query.name)
    val masks = allMask
    val someExp = """Some\(([\S\s]+)\)""".r
    val kvTable = new HBase(Bytes.toBytes(cube + "-" + TableCategory.DimKV))
    def getMember(col: String, memberKey: BitKey): Option[String] = {
      val memberStr = Bytes.toString(kvTable.get(col, memberKey))
      val member = memberStr match {
        case null => None
        case "None" => None
        case "Some()" => Some("")
        case someExp(x) => Some(x)
      }
      member
    }
    //		val dimsKV = if (allDimKV.contains(cube))
    //			allDimKV(cube)
    //		else {
    //			val dimKV = scala.collection.mutable.Map.empty[String, Map[BitKey, Option[String]]]
    //			val someExp = """Some\(([\S\s]+)\)""".r
    //			def getValue(str: String): Option[String] = str match {
    //				case "None" => None
    //				case "Some()" => Some("")
    //				case someExp(x) => Some(x)
    //			}
    //			def saveDims(row: Array[Byte], keyValues: Array[KeyValue]) {
    //				val dimName = Bytes.toString(row)
    //				val members = for {
    //					keyValue <- keyValues
    //					key = Convert.fromByteArray(keyValue.getQualifier)
    //					value = getValue(Bytes.toString(keyValue.getValue))
    //
    //				} yield (key, value)
    //
    //				dimKV += (dimName -> members.toMap)
    //			}
    //
    //			val dimsTable = new HBase(HBase.getTableName(cube, TableCategory.DimKV))
    //			dimsTable.scan(saveDims _)
    //			dimsTable.close
    //			allDimKV += cube -> dimKV.toMap
    //			allDimKV(cube)
    //		}

    var columns = Element.elem("")
    val numberFormat = new DecimalFormat("#,###.00")
    val dimensionMeta = ConfigHelper.getConfig.getStringList(query.name + ".row")
    val measureNames = ConfigHelper.getConfig.getStringList(query.name + ".cell.field")
    val measureTypes = ConfigHelper.getConfig.getStringList(query.name + ".cell.aggType")
    val all = dimensionMeta
    //Sort
    //val orderList=ConfigHelper.getConfig.getStringList(query.name + ".order")
    var sortedAggs = aggs //.toArray
    //		for (order<-orderList){ //only dimension sort support
    //			if(dimensionMeta.contains(order)){
    //				sortedAggs=sortedAggs.sortBy(agg => agg._1.and(masks(order)))
    //			}
    //		}

    for (i <- 0 until dimensionMeta.size) {
      val dimName = dimensionMeta(i)
      var dim = Element.elem(dimName)
      dim = dim above Element.elem("-" * dimName.length())
      for (agg <- sortedAggs) {
        val dimKey = agg._1 and masks.get(dimName).get

        getMember(dimName, dimKey) match {
          case None =>
            log.error("cube (%s) members not contains key(%s) on dim (%s) agg (%s) mask (%s)".
              format(cube, dimKey, dimName, agg._1, masks.get(dimName).get))
          case Some(member) =>
            dim = dim above Element.elem(member)
        }

      }
      columns = columns beside dim
    }
    var seperate = Element.elem("|")
    seperate = seperate above Element.elem("+")
    for (agg <- sortedAggs) {
      seperate = seperate above Element.elem("|")
    }
    columns = columns beside seperate
    for (i <- 0 until measureNames.size) {
      val meaName = measureNames(i)
      val meaType = MeasureType.withName(measureTypes(i))
      val meaMeta = Tuple2(meaName, meaType)
      var mea = Element.elem(meaName)
      mea = mea above Element.elem("-" * meaName.length())
      meaType match {
        case MeasureType.Summary =>
          for (agg <- sortedAggs) {
            mea = mea above Element.elem(
              agg._2.get(meaMeta) match {
                case None => "Null"
                case Some(x) => numberFormat.format(x)
              })
          }
        case MeasureType.Count =>
          for (agg <- aggs) {
            mea = mea above Element.elem(
              agg._2.get(meaMeta) match {
                case None => "Null"
                case Some(x) => numberFormat.format(x)
              })
          }
        case MeasureType.Average =>
          for (agg <- aggs) {
            val cnt = agg._2.get((meaMeta._1, MeasureType.Count))

            val count: BigDecimal = cnt match {
              case None => 0l
              case Some(x) => x
            }
            val summary: BigDecimal = agg._2.get((meaMeta._1, MeasureType.Summary)) match {
              case None => 0l
              case Some(x) => x
            }

            mea = mea above Element.elem(if (count == 0) "" else numberFormat.format(summary / count))
          }
      }

      columns = columns beside mea
    }
    kvTable.close
    log.info("end render query (%s)".format(query))
    columns.toString
  }
}
