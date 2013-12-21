package bulu.util

import scala.collection.JavaConversions._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.{ HTable, HBaseAdmin, Get, Put, Delete, Scan, Result }
import org.apache.hadoop.hbase.{
	HBaseConfiguration,
	HTableDescriptor,
	HColumnDescriptor
}
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.filter.RowFilter
import org.apache.hadoop.hbase.filter.BitComparator
import org.apache.hadoop.hbase.filter.CompareFilter
import org.apache.hadoop.hbase.client.Increment
import bulu.core.BitKey
import bulu.util.TableCategory._

class HBase(tableName: Array[Byte], confPath: String = null) {
	val ba1 = Bytes.toBytes(1l);
	def this(tableString: String) {
		this(HBase.toBytes(tableString))
	}
	val table = new HTable(HBase.getConf(confPath), tableName)

	def putList(list: List[Put]) {
		table.put(list)
	}

	def put(row: Array[Byte], family: Array[Byte], maps: Map[Array[Byte], Array[Byte]]) {
		val put = new Put(row)
		for (map <- maps) {
			put.add(family, map._1, map._2)
		}
		table.put(put)

	}

	def put(row: Array[Byte], family: Array[Byte], qualifier: Array[Byte], value: Array[Byte]) {
		val put = new Put(row)
		put.add(family, qualifier, value)
		table.put(put)
	}
	//	def put(key: BitKey, values: Map[String, Any]) {
	//		val put = new Put(Convert.toByteArray(key))
	//		for (value <- values) {
	//			put.add(Bytes.toBytes(HBase.DefaultFamily),
	//				Convert.measureName2Qualifier(value._1, HBase.Count), ba1)
	//			put.add(Bytes.toBytes(HBase.DefaultFamily),
	//				Convert.measureName2Qualifier(value._1, HBase.Summary), HBase.toBytes(Convert.toLong(value._2)))
	//		}
	//		table.put(put)
	//	}
	//	def incr(key: BitKey, values: Map[String, Any]) {
	//		val increment = new Increment(Convert.toByteArray(key))
	//		for (value <- values) {
	//
	//			increment.addColumn(Bytes.toBytes(HBase.DefaultFamily),
	//				Convert.measureName2Qualifier(value._1, "count"), 1)
	//			increment.addColumn(Bytes.toBytes(HBase.DefaultFamily),
	//				Convert.measureName2Qualifier(value._1, "sum"), (Convert.toLong(value._2)))
	//		}
	//		table.increment(increment)
	//	}
	def incr(row: Any, family: Any, qualifier: Any, value: Long) {
		table.incrementColumnValue(HBase.toBytes(row), HBase.toBytes(family),
			HBase.toBytes(qualifier), value)
	}
	def get(key: BitKey, qualifiers: List[Array[Byte]], func: (BitKey, Array[KeyValue]) => Unit) {
		val get = new Get(Convert.toByteArray(key))
		//		qualifiers.foreach(f =>
		//			get.addColumn(Bytes.toBytes(HBase.DefaultFamily), f)
		//		)
		func(key, table.get(get).raw)

	}
	def get(row: String, qualifier: String): Array[Byte] = {
		get(Bytes.toBytes(row), Bytes.toBytes(HBase.DefaultFamily), Bytes.toBytes(qualifier))
	}
	def get(row: String, qualifier: BitKey): Array[Byte] = {
		get(Bytes.toBytes(row), Bytes.toBytes(HBase.DefaultFamily), Convert.toByteArray(qualifier))
	}
	def get(row: Array[Byte], family: Array[Byte], qualifier: Array[Byte]): Array[Byte] = {
		val result = table.get(new Get(row))
		return result.getValue(family, qualifier)
	}
	def getCount(row: Array[Byte], family: Array[Byte]): Int = {
		val result = table.get(new Get(row))
		if (result == null) 0
		else result.size()
	}
	def getAny(row: Any, family: Any, qualifier: Any): Array[Byte] = {
		val result = table.get(new Get(HBase.toBytes(row)))
		return result.getValue(HBase.toBytes(family), HBase.toBytes(qualifier))
	}
	def existsKey(key: BitKey) = table.exists(new Get(Convert.toByteArray(key)))
	def exists(row: Any): Boolean = table.exists(new Get(HBase.toBytes(row)))

	def exists(row: Any, family: Any, qualifier: Any): Boolean = {
		val get = new Get(HBase.toBytes(row))
		get.addColumn(HBase.toBytes(family), HBase.toBytes(qualifier))
		table.exists(get)
	}

	def getLong(row: Any, family: Any, qualifier: Any): Long = {
		val res = getAny(row, family, qualifier)
		if (res == null)
			0
		else
			Bytes.toLong(res)
	}

	def getInt(row: Any, family: Any, qualifier: Any): Int = {
		val res = getAny(row, family, qualifier)
		if (res == null)
			0
		else
			Bytes.toInt(res)
	}
	def getDouble(row: Any, family: Any, qualifier: Any): Double =
		Bytes.toDouble(getAny(row, family, qualifier))

	def getString(row: Any, family: Any, qualifier: Any): String =
		Bytes.toString(getAny(row, family, qualifier))

	def get(row: Any,
		func: (Array[Byte], Array[Byte], Array[Byte]) => Unit) {
		for (kv <- table.get(new Get(HBase.toBytes(row))).raw) {
			func(kv.getFamily, kv.getQualifier, kv.getValue)
		}
	}

	def getBytes(row: Any,
		func: (String, String, Array[Byte]) => Unit) {
		for (kv <- table.get(new Get(HBase.toBytes(row))).raw) {
			func(Bytes.toString(kv.getFamily), Bytes.toString(kv.getQualifier), kv.getValue)
		}
	}

	def getString(row: Any,
		func: (String, String, String) => Unit) {
		for (kv <- table.get(new Get(HBase.toBytes(row))).raw) {
			func(Bytes.toString(kv.getFamily), Bytes.toString(kv.getQualifier),
				Bytes.toString(kv.getValue))
		}
	}

	def getLong(row: Any,
		func: (String, String, Long) => Unit) {
		for (kv <- table.get(new Get(HBase.toBytes(row))).raw) {
			func(Bytes.toString(kv.getFamily), Bytes.toString(kv.getQualifier),
				Bytes.toLong(kv.getValue))
		}
	}

	def getDouble(row: Any,
		func: (String, String, Double) => Unit) {
		for (kv <- table.get(new Get(HBase.toBytes(row))).raw) {
			func(Bytes.toString(kv.getFamily), Bytes.toString(kv.getQualifier),
				Bytes.toDouble(kv.getValue))
		}
	}
	def scan(func: (Array[Byte], Array[Byte], Array[Byte], Array[Byte]) => Unit) {
		val rs = table.getScanner(new Scan())
		try {
			for (item <- rs) {
				for (kv <- item.raw) {
					func(item.getRow, kv.getFamily, kv.getQualifier, kv.getValue)
				}
			}
		}
		finally {
			rs.close
		}
	}

	def scan(qulifiers: List[Array[Byte]], start: BitKey, end: BitKey, filters: List[BitKey],
		func: (Array[Byte], Array[KeyValue], List[BitKey]) => Unit) {

		val scan = new Scan(Convert.toByteArray(start), Convert.toByteArray(end))
		//		qulifiers.foreach(f =>
		//			scan.addColumn(Bytes.toBytes(HBase.defaultFamily(0)), f)
		//		)
		//TODO: use custome filter instead
		//		val filter1 = new RowFilter(CompareFilter.CompareOp.NO_OP,
		//			new BitComparator(Convert.toByteArray(filterKey),BitComparator.BitwiseOp.AND));
		//		scan.setFilter(filter1);

		val rs = table.getScanner(scan)
		try {
			for (item <- rs) {
				func(item.getRow, item.raw, filters)

			}
		}
		finally {
			rs.close
		}
	}

	def scan(func: (Array[Byte], Array[KeyValue]) => Unit) {
		val rs = table.getScanner(new Scan())
		try {
			for (item <- rs) {
				func(item.getRow, item.raw)

			}
		}
		finally {
			rs.close
		}
	}
	def scan(startRow: Array[Byte], endRow: Array[Byte],
		func: (Array[Byte], Array[KeyValue]) => Unit):Int= {
		var count=0
		val rs = table.getScanner(new Scan(startRow, endRow))
		try {
			for (item <- rs) {
				func(item.getRow, item.raw)
				count+=1

			}
		}
		finally {
			rs.close
		}
		count
	}
	def getReionsServerAdd() = {
		table.getRegionsInfo()
		
	}
	val localRegions={
		val hostName=ToolHelper.getHostName
		
		val regions=table.getRegionsInfo()
		//assert(regions.size()>0)
		//for((info,server)<-regions)println("server's name=%s, this.hostName=%s".format(server.getHostname(),hostName))
		val startEndKeys=for((info,server)<-regions if(server.getHostname().toUpperCase().startsWith(hostName.toUpperCase())))
				yield (info.getStartKey(), info.getEndKey())
		
		startEndKeys.toList.sortWith((x,y)=>Convert.fromByteArray(x._1).compareTo(Convert.fromByteArray(y._1))>=0)
		
	}
	def getStartEndKeys = table.getStartEndKeys()
	def scan(startRow: Any, endRow: Any,
		func: (Array[Byte], Array[Byte], Array[Byte], Array[Byte]) => Unit) {
		val rs = table.getScanner(new Scan(HBase.toBytes(startRow), HBase.toBytes(endRow)))
		try {
			for (item <- rs) {
				for (kv <- item.raw) {
					func(item.getRow, kv.getFamily, kv.getQualifier, kv.getValue)
				}
			}
		}
		finally {
			rs.close
		}
	}

	def scan(startRow: Any,
		func: (Array[Byte], Array[Byte], Array[Byte], Array[Byte]) => Unit) {
		scan(startRow, getNextBytes(HBase.toBytes(startRow)), func)
	}

	def scanBytes(startRow: Any, endRow: Any,
		func: (String, String, String, Array[Byte]) => Unit) {
		scan(startRow, endRow, (row: Array[Byte], family: Array[Byte],
			qualifier: Array[Byte], value: Array[Byte]) => {
			func(Bytes.toString(row), Bytes.toString(family),
				Bytes.toString(qualifier), value)
		})
	}

	def scanBytes(startRow: Any,
		func: (String, String, String, Array[Byte]) => Unit) {
		scanBytes(startRow, getNextBytes(HBase.toBytes(startRow)), func)
	}

	def scanString(startRow: Any, endRow: Any,
		func: (String, String, String, String) => Unit) {
		scan(startRow, endRow, (row: Array[Byte], family: Array[Byte],
			qualifier: Array[Byte], value: Array[Byte]) => {
			func(Bytes.toString(row), Bytes.toString(family),
				Bytes.toString(qualifier), Bytes.toString(value))
		})
	}

	def scanString(startRow: Any,
		func: (String, String, String, String) => Unit) {
		scanString(startRow, getNextBytes(HBase.toBytes(startRow)), func)
	}

	def scanLong(startRow: Any, endRow: Any,
		func: (String, String, String, Long) => Unit) {
		scan(startRow, endRow, (row: Array[Byte], family: Array[Byte],
			qualifier: Array[Byte], value: Array[Byte]) => {
			func(Bytes.toString(row), Bytes.toString(family),
				Bytes.toString(qualifier), Bytes.toLong(value))
		})
	}

	def scanLong(startRow: Any,
		func: (String, String, String, Long) => Unit) {
		scanLong(startRow, getNextBytes(HBase.toBytes(startRow)), func)
	}

	def scanDouble(startRow: Any, endRow: Any,
		func: (String, String, String, Double) => Unit) {
		scan(startRow, endRow, (row: Array[Byte], family: Array[Byte],
			qualifier: Array[Byte], value: Array[Byte]) => {
			func(Bytes.toString(row), Bytes.toString(family),
				Bytes.toString(qualifier), Bytes.toDouble(value))
		})
	}

	def scanRow(startRow: Any, endRow: Any, func: (Result) => Unit) {
		table.getScanner(new Scan(HBase.toBytes(startRow), HBase.toBytes(endRow)))
		val rs = table.getScanner(new Scan(HBase.toBytes(startRow), HBase.toBytes(endRow)))
		try {
			for (item <- rs)
				func(item)
		}
		finally {
			rs.close
		}
	}

	def scanRow(startRow: Any, func: (Result) => Unit) {
		val startBytes = HBase.toBytes(startRow)
		scanRow(startBytes, getNextBytes(startBytes), func)
	}

	def scanDouble(startRow: Any,
		func: (String, String, String, Double) => Unit) {
		scanDouble(startRow, getNextBytes(HBase.toBytes(startRow)), func)
	}

	def delete(row: Any) {
		val delete = new Delete(HBase.toBytes(row))
		table.delete(delete)
	}

	def delete(row: Any, family: Any) {
		val delete = new Delete(HBase.toBytes(row))
		delete.deleteFamily(HBase.toBytes(family))
		table.delete(delete)
	}

	def delete(row: Any, family: Any, qualifier: Any) {
		val delete = new Delete(HBase.toBytes(row))
		delete.deleteColumn(HBase.toBytes(family), HBase.toBytes(qualifier))
		table.delete(delete)
	}
	def setOptimizeForBuild() {
		table.setAutoFlush(false)
		table.setWriteBufferSize(ConfigHelper.conf.getInt("hbase.write_buffer_size"))
	}
	def commit = table.flushCommits

	def close = table.close

	private def getNextBytes(bytes: Array[Byte]): Array[Byte] = {
		var nextBytes = new Array[Byte](bytes.length)
		nextBytes(nextBytes.length - 1) = (nextBytes(nextBytes.length - 1) + 1).toByte
		nextBytes
	}

}

object HBase {

	val DEFAULT_CONF_PATH = "hbase-site.xml"
	val DEFAULT_CONF = new HBaseConfiguration
	DEFAULT_CONF.addResource(new Path(DEFAULT_CONF_PATH))

	val DefaultFamily = "f1"
	val DefaultMaskRowId = "MaskRow"
	val MaxKeyCursor = "MaxKeyCursor"
	def getPut(row: Array[Byte], family: Array[Byte], maps: Map[Array[Byte], Array[Byte]]) = {
		val put = new Put(row)
		for (map <- maps) {
			put.add(family, map._1, map._2)
		}
		put

	}
	def loadDimVK(cube: String): Map[String, Map[Option[String], BitKey]] = {
		val result = scala.collection.mutable.Map.empty[String, Map[Option[String], BitKey]]
		val someExp = """Some\(([\S\s]+)\)""".r
		def getValue(str: String): Option[String] = str match {
			case "None" => None
			case "Some()" => Some("")
			case someExp(x) => Some(x)

		}
		def saveDims(row: Array[Byte], keyValues: Array[KeyValue]) {
			val dimName = Bytes.toString(row)
			val members = for {
				keyValue <- keyValues
				key = getValue(Bytes.toString(keyValue.getQualifier))
				value = Convert.fromByteArray(keyValue.getValue)

			} yield (key, value)

			result += (dimName -> members.toMap)
		}
		val dimsTable = new HBase(HBase.getTableName(cube, TableCategory.DimVK));
		dimsTable.scan(saveDims _)
		dimsTable.close

		result.toMap

	}

	def getTableName(table: String, cat: TableCategory, key: BitKey = BitKey.EMPTY): Array[Byte] = {
		cat match {
			case TableCategory.DimKV =>
				Bytes.toBytes(table + "-" + DimKV.toString)
			case TableCategory.DimVK =>
				Bytes.toBytes(table + "-" + DimVK.toString)
			case TableCategory.Mask =>
				Bytes.toBytes(table + "-" + Mask.toString)
			case TableCategory.CuboidBase =>
				Bytes.toBytes(table + "-" + CuboidBase.toString)
			//			case TableCategory.CuboidOther =>
			//				Bytes.toBytes(table + "-" + CuboidOther.toString)
		}
	}
	def toBigDecimal(value: Any): BigDecimal = {
		if (value == null)
			return null
		if (value.isInstanceOf[Int])
			return BigDecimal(value.asInstanceOf[Int])
		if (value.isInstanceOf[Long])
			return BigDecimal(value.asInstanceOf[Long])
		if (value.isInstanceOf[Float])
			return BigDecimal(value.asInstanceOf[Float].toDouble)
		if (value.isInstanceOf[Double])
			return BigDecimal(value.asInstanceOf[Double])

		return BigDecimal(value.toString)
	}
	def bytes2BigDecimal(bytes:Array[Byte]):BigDecimal=Bytes.toBigDecimal(bytes)
	def bigDecimal2Bytes(value:java.math.BigDecimal):Array[Byte]=Bytes.toBytes(value)
	private def toBytes(value: Any): Array[Byte] = {
		if (value == null)
			return null
		if (value.isInstanceOf[BigDecimal])
			return Bytes.toBytes(value.asInstanceOf[java.math.BigDecimal])
		if (value.isInstanceOf[Int])
			return Bytes.toBytes(value.asInstanceOf[Int].toLong)
		if (value.isInstanceOf[Long])
			return Bytes.toBytes(value.asInstanceOf[Long])
		if (value.isInstanceOf[Float])
			return Bytes.toBytes(value.asInstanceOf[Float].toDouble)
		if (value.isInstanceOf[Double])
			return Bytes.toBytes(value.asInstanceOf[Double])

		return Bytes.toBytes(value.toString)
	}

	def getConf(confPath: String = null): HBaseConfiguration = {
		if (confPath == null) {
			DEFAULT_CONF
		}
		else {
			val conf = new HBaseConfiguration()
			conf.addResource(new Path(confPath))
			conf
		}
	}

	def create(tableName: String, families: List[String]) {

		val hAdmin = new HBaseAdmin(getConf())
		val descriptor = new HTableDescriptor(tableName)
		for (family <- families)
			descriptor.addFamily(new HColumnDescriptor(Bytes.toBytes(family)))

		hAdmin.createTable(descriptor)
		hAdmin.close()
	}
	def create(tableName: String, families: List[String],
		start: BitKey, end: BitKey, numOfRegion: Int,
		conf: HBaseConfiguration = getConf()) {

		val hAdmin = new HBaseAdmin(conf)
		val descriptor = new HTableDescriptor(tableName)
		for (family <- families)
			descriptor.addFamily(new HColumnDescriptor(Bytes.toBytes(family)))
		val startKey = Convert.toByteArray(start)
		val endKey = Convert.toByteArray(end)

		hAdmin.createTable(descriptor) //, startKey, endKey, numOfRegion);

		hAdmin.close()
		println("create Table cuboid base: [%s]-[%s],num=%s".format(start, end, numOfRegion))
	}

	def drop(tableName: String, conf: HBaseConfiguration = getConf()) {
		val hAdmin = new HBaseAdmin(conf)
		hAdmin.disableTable(tableName)
		hAdmin.deleteTable(tableName)

		hAdmin.close()
	}

	def recreate(tableName: String, conf: HBaseConfiguration = getConf()) {
		val hAdmin = new HBaseAdmin(conf)
		val descriptor = hAdmin.getTableDescriptor(Bytes.toBytes(tableName))
		drop(tableName)
		hAdmin.createTable(descriptor)
		hAdmin.close()
	}

	def exists(tableName: String, conf: HBaseConfiguration = getConf()): Boolean = {
		val hAdmin = new HBaseAdmin(conf)
		val result = hAdmin.tableExists(tableName)
		hAdmin.close()
		result
	}
	def exists(tableName: Array[Byte]): Boolean = {
		val hAdmin = new HBaseAdmin(getConf())
		val result = hAdmin.tableExists(tableName)
		hAdmin.close()
		result
	}
	def disable(tableName: String, conf: HBaseConfiguration = getConf()) {
		val hAdmin = new HBaseAdmin(conf)
		hAdmin.disableTable(tableName)
		hAdmin.close()
	}

	def enable(tableName: String, conf: HBaseConfiguration = getConf()) {
		val hAdmin = new HBaseAdmin(conf)
		hAdmin.enableTable(tableName)
		hAdmin.close()
	}

	def disableWork(tableName: String, func: () => Unit, conf: HBaseConfiguration = getConf()) {
		val hAdmin = new HBaseAdmin(conf)
		hAdmin.disableTable(tableName)
		func.apply
		hAdmin.enableTable(tableName)
		hAdmin.close()
	}

	def list(conf: HBaseConfiguration = getConf()): List[String] = {
		val hAdmin = new HBaseAdmin(conf)
		var nameList = List[String]()
		hAdmin.listTables.foreach(table => { nameList +:= table.getNameAsString })
		hAdmin.close()
		nameList

	}

	def addColumn(tableName: String, familyName: String, conf: HBaseConfiguration = getConf()) {
		val hAdmin = new HBaseAdmin(conf)
		val column = new HColumnDescriptor(familyName)
		hAdmin.disableTable(tableName)
		hAdmin.addColumn(tableName, column)
		hAdmin.enableTable(tableName)
		hAdmin.close()
	}

	def deleteColumn(tableName: String, familyName: String, conf: HBaseConfiguration = getConf()) {
		val hAdmin = new HBaseAdmin(conf)
		hAdmin.disableTable(tableName)
		hAdmin.deleteColumn(tableName, familyName)
		hAdmin.enableTable(tableName)
		hAdmin.close()
	}

}