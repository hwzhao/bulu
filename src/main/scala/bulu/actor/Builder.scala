package bulu.actor
import akka.actor.Actor
import akka.actor.Props
import akka.routing.RoundRobinRouter
import akka.actor.ActorRef
import akka.event.Logging
import bulu.util._
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.util.Bytes
import akka.actor.ActorLogging
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.KeyValue
import bulu.actor.build._
import bulu.core.Field
import java.sql.DriverManager
import java.sql.ResultSet
import scala.collection.mutable.ArrayBuffer
import bulu.util.SendLeftRecords

/**
 * accumulate the record for sending in a batch
 */
class RecordWrapper(cube:String, worker:ActorRef) extends Actor with ActorLogging {
	val cache=ArrayBuffer.empty[Map[String, Option[Any]]]
	def receive: Receive = {
		case Record(rec) =>
			log.debug("cube (%s) worker (%s) get record(%s)".
					format(cube,worker, rec))
			cache+=rec
			if(cache.size==ConfigHelper.getRecordThreshold(cube)){
				worker ! RecordList(cube, cache.toList)
				cache.clear
			}
		case SendLeftRecords =>
			log.debug("cube (%s) worker (%s) send left records(%s)".
					format(cube,worker, cache))
			worker ! RecordList(cube, cache.toList)
	}
			
	
}
class Builder(cube:String, workers:Seq[ActorRef]) extends Actor with ActorLogging {
	//val reduceActor = context.actorOf(Props[ReduceActor].withRouter(RoundRobinRouter(nrOfInstances = 5)), name = "reduce")

	//	lazy val dimVK = loadDimVK("hive")
	//	val all = List(fetcher, dimensionManager, aggregator)
	//	var switcher = 0
	//	context.
	//	
	var cubeSinker:ActorRef=null
	def receive: Receive = {
		case BuildStart =>
			if (workers.size == 0) throw new Exception("There is no worker existing!")
			else {
				ensureTable(cube)
				log.info("begin build cube (%s) step 1 fetch dimension with %s workers".format(cube, workers.size))
				val dimCount = ConfigHelper.getDimensionCount(cube)
				val dimensionCreator = context.child("dimensionCreator") match {
					case None => context.actorOf(Props(new DimensionSinker(dimCount, self, workers)), name = "dimensionSinker")
					case Some(x) => x
				}

				for (dimIndex <- 0 until dimCount) {
					val field = ConfigHelper.getDimension(cube, dimIndex)
					//workers(dimIndex % workers.size) ! FetchDimension(cube, field, dimensionCreator)
					workers(0) ! FetchDimension(cube, field, dimensionCreator)
				}
			}
		//dimensionCreator ! CubeMetaData(cube)
		case df:DimensionFinshed =>//(cube, partitionField, list, workers) =>
			log.info("begin build cube (%s) step 2 fetch raw data with %s workers partition is (%s)".
				format(df.cube, df.workers.size, df.partitionField))
			cubeSinker = context.actorOf(Props(new CubeSinker(df.workers.size, self)), name = "cubeSinker-%s-Worker" format df.workers.size)
			val workerMappers=scala.collection.mutable.Map.empty[ActorRef, ActorRef]
			val wraper=ArrayBuffer.empty[ActorRef]
			for(worker<-df.workers){
				val mapper=context.actorOf(Props(new RecordWrapper(df.cube,worker)))
				workerMappers+=worker->mapper
				wraper+=mapper
			}
			
			
			val mapperCounts=fetchRawdata(df,wraper.toSeq)
			
			wraper.foreach( _ ! SendLeftRecords)
			Thread.sleep(500)
			
			for(w <- df.workers){
				w ! RawDataFinished(df.cube,cubeSinker, mapperCounts(workerMappers(w)))
			}
			//log.info("end build cube (%s)" format df.cube)
			//sender ! RawDataFinished
		//			val conditions = Array.fill(workers.size){""}// new Array[String](workers.size)
		//			
		//			for (i <- 0 until list.size) {
		//				val mod = i % workers.size
		//				if (conditions(mod).isEmpty()) {
		//					conditions(mod) = partitionField.column + "='" + list(i).get + "'"
		//				}
		//				else {
		//					conditions(mod) += " OR " + partitionField.column + "='" + list(i).get + "'"
		//				}
		//			}
		//			val cubeSinker = context.actorOf(Props(new CubeSinker(workers.size, self)), name = "cubeSinker")
		//			for (i <- 0 until workers.size) {
		//				workers(i) ! RawData(cube, conditions(i), cubeSinker)
		//				//				fetcher ! RawData(cube,condition)
		//			}
		case CubeFinshed(cube,count) =>
			context.stop(cubeSinker)
			log.info("end build cube (%s) with cells (%s)".format( cube,count))
		//		case nf: NodeFinished => aggregator ! nf
		//		case rec: Record => dimensionManager ! rec
		//		case f: FetchFinished => dimensionManager ! f
		//		case aggNode: Cell => aggregator ! aggNode
		//		case save: Save =>
		//			//					dimensionManager ! save
		//			aggregator ! save
		//		case sinkcell:SinkCell => cellSinker ! sinkcell
		//		case result: StatusResult =>
		//
		//			aggregator forward result

	}

	def fetchRawdata(info: DimensionFinshed,wrappers: Seq[ActorRef]) = {
		def getFunc(res: ResultSet, dataType: String) = {
			dataType match {
				case "String" => res.getString(_: String)
				case "Int" => res.getInt(_: String)
				case "Double" => res.getDouble(_: String)
				case "Float" => res.getFloat(_: String)
				case "Long" => res.getLong(_: String)
				case "Boolean" => res.getBoolean(_: String)
				case "BigDecimal" => res.getBigDecimal(_: String)
			}
		}
		
		val mapper=scala.collection.mutable.Map.empty[Option[String],Int]
		val m=info.workers.size
		val lists=new Array[ArrayBuffer[Option[String]]](m)
		for (i<-0 until m){
			lists(i)=ArrayBuffer.empty[Option[String]]
		}
		for(i <- 0 until info.list.size){
			val mod=i % m
			mapper+=info.list(i)->(mod)
			lists(mod)+=info.list(i)
		}
		for(i<-0 until m){
			info.workers(i) ! RawDataBegan(info.cube, cubeSinker,lists(i).toList,info.partitionField,i,m)
		}
		val driver = ConfigHelper.getDriver(info.cube)
		val url = ConfigHelper.getUrl(info.cube)
		var fetchCount = 0
		try {
			Class.forName(driver);
		}
		catch {
			case e: ClassNotFoundException =>
				e.printStackTrace();
				log.error(e, "");
		}
		val con = DriverManager.getConnection(url, "", "")
		val stmt = con.createStatement()
		val sql = ConfigHelper.getSql(info.cube) //+ " Where " + condition
		log.info("begin execute cube (%s) 's raw data query (%s)".format(info.cube,sql))
		val res = stmt.executeQuery(sql)
		log.info("end execute cube (%s) 's raw data query (%s)".format(info.cube,sql))
		res.setFetchSize(ConfigHelper.getFetchSize(info.cube))
		val mapperCounts=scala.collection.mutable.Map.empty[ActorRef, Int]
		for(wrapper <- wrappers){
			mapperCounts+=wrapper->0
		}
		val fields = ConfigHelper.getAllFields(info.cube)
		while (res.next()) {
			val rec = for (field <- fields)
				yield field.column -> { val result=getFunc(res, field.dataType)(field.column)
					if(res.wasNull())None
					else Some(result)
				}
			val record=rec.toMap
			val act=wrappers(mapper(record(info.partitionField.column).asInstanceOf[Option[String]]))
			act ! Record(record)
			mapperCounts(act)+=1
			fetchCount += 1
		}

		res.close()
		stmt.close()
		con.close()
		log.info("cube (%s) 's raw data has %s record".
			format(info.cube, fetchCount))
		mapperCounts

	}

	def ensureTable(cube: String) {

		TableCategory.values.foreach(
			value => {
				val tableName = HBase.getTableName(cube, value)
				if (ConfigHelper.getInitial(cube)) {
					if (HBase.exists(Bytes.toString(tableName))) HBase.drop(Bytes.toString(tableName));
				}
				//!value.toString().startsWith("Cuboid") &&
				if (!HBase.exists(Bytes.toString(tableName))) {
					HBase.create(Bytes.toString(tableName), List(HBase.DefaultFamily));

				}
			})
		//		if (Config.getConfig.getBoolean(cube + "." + "init_at_first")) {
		//			val dimMask = new HBase(HBase.getTableName(cube, TableCategory.Mask))
		//			
		//		}

		//		val dimKVTable = HBase.getTableName(table, TableCategory.DimKV)
		//		val dimVKTable = HBase.getTableName(table, TableCategory.DimVK)
		//		val masksTable = HBase.getTableName(table, TableCategory.Mask)
		//		val baseCuboidTable = HBase.getTableName(table, TableCategory.CuboidBase)
		//		val otherCuboidTable = HBase.getTableName(table, TableCategory.CuboidOther)
		//		if (Config.getConfig.getBoolean(table + "." + "init_at_first")) {
		//			if (HBase.exists(Bytes.toString(dimVKTable))) HBase.drop(Bytes.toString(dimVKTable));
		//			if (HBase.exists(Bytes.toString(dimKVTable))) HBase.drop(Bytes.toString(dimKVTable));
		//			if (HBase.exists(Bytes.toString(masksTable))) HBase.drop(Bytes.toString(masksTable));
		//			if (HBase.exists(Bytes.toString(baseCuboidTable))) HBase.drop(Bytes.toString(baseCuboidTable));
		//		}
		//		if (!HBase.exists(Bytes.toString(dimVKTable))) {
		//			HBase.create(Bytes.toString(dimVKTable), HBase.defaultFamily);
		//		}
		//		if (!HBase.exists(Bytes.toString(dimKVTable))) {
		//			HBase.create(Bytes.toString(dimKVTable), HBase.defaultFamily);
		//		}
		//		if (!HBase.exists(Bytes.toString(masksTable))) {
		//			HBase.create(Bytes.toString(masksTable), HBase.defaultFamily);
		//		}
		//		if (!HBase.exists(Bytes.toString(baseCuboidTable))) {
		//			HBase.create(Bytes.toString(baseCuboidTable), HBase.defaultFamily);
		//		}
	}

}
