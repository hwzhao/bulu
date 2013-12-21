package bulu.util

import bulu.core.Field
import bulu.core.CubeMetaType
import bulu.core.MeasureType

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase.HBaseConfiguration
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

object ConfigHelper {
	lazy val actorsForWorker=conf.getInt("bulu.actors_for_worker")
	
	val sharding: Map[Int, List[(String, String)]] = Map(
		1 -> List(("0", "0")),
		2 -> List(("1", "0"), ("1", "1")),
		4 -> List(("11", "00"), ("11", "01"), ("11", "10"), ("11", "11")),
		8 -> List(("111", "000"), ("111", "001"), ("111", "010"), ("111", "011"),
			("111", "100"), ("111", "101"), ("111", "110"), ("111", "111")),
		16 -> List(("1111", "0000"), ("1111", "0001"), ("1111", "0010"), ("1111", "0011"),
			("1111", "0100"), ("1111", "0101"), ("1111", "0110"), ("1111", "0111"),
			("1111", "1000"), ("1111", "1001"), ("1111", "1010"), ("1111", "1011"),
			("1111", "1100"), ("1111", "1101"), ("1111", "1110"), ("1111", "1111")),
		32 -> List(("11111", "00000"), ("11111", "00001"), ("11111", "00010"), ("11111", "00011"),
			("11111", "00100"), ("11111", "00101"), ("11111", "00110"), ("11111", "00111"),
			("11111", "01000"), ("11111", "01001"), ("11111", "01010"), ("11111", "01011"),
			("11111", "01100"), ("11111", "01101"), ("11111", "01110"), ("11111", "01111"),
			("11111", "10000"), ("11111", "10001"), ("11111", "10010"), ("11111", "10011"),
			("11111", "10100"), ("11111", "10101"), ("11111", "10110"), ("11111", "10111"),
			("11111", "11000"), ("11111", "11001"), ("11111", "11010"), ("11111", "11011"),
			("11111", "11100"), ("11111", "11101"), ("11111", "11110"), ("11111", "11111"))
	)
	val conf = ConfigFactory.load()
	def getIsWriteDisk(query:String)=conf.getBoolean(query + "." + "write_to_disk")
	def getMeasureFields(query:String)= conf.getStringList(query +".cell.field")
	def getInitial(cube: String) = conf.getBoolean(cube + "." + "init_at_first")
	def getRecordThreshold(cube: String) = conf.getInt(cube + "." + "record_threshold")
	def getCellThreshold(cube: String) = conf.getInt(cube + "." + "cell_threshold")
	def getDimensionCount(cube: String): Int = {
		conf.getStringList(cube + ".metadata.dimensions").filter(_ == "True").size
	}
	def getDriver(cube: String): String = conf.getString(cube + ".driver")

	def getUrl(cube: String): String = conf.getString(cube + ".url")

	def getFetchSize(cube: String) = conf.getInt(cube + ".fetchSize")
	def getSql(cube: String) = conf.getString(cube + ".statement")
	def getDimensionNames(cube: String): List[String] = {
		val result = ArrayBuffer.empty[String]
		val dims = conf.getStringList(cube + ".metadata.dimensions")
		val fields = conf.getStringList(cube + ".metadata.fields")
		for (i <- 0 until dims.size()) {
			if (dims(i) == "True") {
				result += fields(i)
			}
		}
		result.toList
	}
	def getMeasureNames(cube: String): List[String] = {
		val result = ArrayBuffer.empty[String]
		val measures = conf.getStringList(cube + ".metadata.measures")
		val fields = conf.getStringList(cube + ".metadata.fields")
		for (i <- 0 until measures.size()) {
			if (measures(i) == "True") {
				result += fields(i)
			}
		}
		result.toList
	}
	def getDimension(cube: String, index: Int): Field = {
		var cur = -1
		val dims = conf.getStringList(cube + ".metadata.dimensions")
		for (i <- 0 until dims.size) {
			if (dims.get(i) == "True") {
				cur += 1
				if (cur == index) {
					val field = Field(conf.getStringList(cube + ".metadata.tables").get(i),
						conf.getStringList(cube + ".metadata.fields").get(i),
						conf.getStringList(cube + ".metadata.dataTypes").get(i),
						CubeMetaType.Dimension)
					return field
				}
			}
		}
		return null
	}
	def getBuildMeasures(cube: String): List[(String, MeasureType.MeasureType)] = {
		val result = ArrayBuffer.empty[(String, MeasureType.MeasureType)]
		val fields = conf.getStringList(cube + ".metadata.fields")
		val measures = conf.getStringList(cube + ".metadata.measures")
		for (i <- 0 until measures.size) {
			if (measures(i) == "True") {
				val col = fields.get(i)
				result += Tuple2(col, MeasureType.Count)
				result += Tuple2(col, MeasureType.Summary)
			}
		}
		result.toList
	}
	def getAllFields(cube: String) = {
		val result = ArrayBuffer.empty[Field]

		val fields = conf.getStringList(cube + ".metadata.fields")
		val types = conf.getStringList(cube + ".metadata.dataTypes")
		val dims = conf.getStringList(cube + ".metadata.dimensions")
		val measures = conf.getStringList(cube + ".metadata.measures")
		val tables = conf.getStringList(cube + ".metadata.tables")
		for (i <- 0 until fields.size()) {
			val cubeType =
				if (dims(i) == "True") CubeMetaType.Dimension
				else if (measures(i) == "True") CubeMetaType.Measure
				else CubeMetaType.Attribute
			result += Field(tables(i), fields(i), types(i), cubeType)
		}
		result.toList
	}
	def getPartition(cube: String) = {
		conf.getString(cube + ".partition")
	}
	//end of build
//	def getQueryPartition(cube: String) = conf.getString(cube + ".queryPartition")

	def getBaseCuboidName(cube: String) = cube + "-" + TableCategory.CuboidBase
	def getQueryCube(queryName: String) = conf.getString(queryName + ".cube")
	def getQueryCuboid(query: String) = conf.getStringList(query + ".usedCuboid")
	def getConfig = conf
	val FetcherCount = 16
	//	object hive {
	//		var dimsKV = Map.empty[String, Map[BitKey, Option[String]]]
	//		var dimsVK = Map.empty[String, Map[Option[String], BitKey]]
	//		var masks = Map.empty[String, BitKey]
	//		//		def getSql = """select ss_item_sk, ss_quantity
	//		//		from  store_sales
	//		//		
	//		//		"""
	//		//		//order by ss_item_sk
	//		//
	//		//		def getFields:List[(String,String)] = List( ( "ss_item_sk", "String" ), ( "ss_quantity", "Int" ) )
	//		//		def getDimensions :List[String]= List( "ss_item_sk" )
	//		//		def getMeasures:List[String] = List( "ss_quantity" )
	//		def getSql = """
	//			select p_channel_email,p_channel_event,cd_gender,cd_marital_status,cd_education_status,
	//				i_item_id,d_year, ss_quantity, ss_list_price, ss_coupon_amt, ss_sales_price 
	//			from  store_sales join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk) 
	//				join item on (store_sales.ss_item_sk = item.i_item_sk) 
	//				join customer_demographics on (store_sales.ss_cdemo_sk = customer_demographics.cd_demo_sk) 
	//				join promotion on (store_sales.ss_promo_sk = promotion.p_promo_sk)
	//			
	//		"""
	//		//[N, N, M, W, Primary, AAAAAAAAMIPDAAAA, 2000, 77, 50.25, 0.0, 24.62]
	//		def getFields: List[(String, String)] = List(("i_item_id", "String"), ("cd_gender", "String"), ("cd_marital_status", "String"),
	//			("cd_education_status", "String"), ("p_channel_email", "String"), ("p_channel_event", "String"),
	//			("d_year", "String"), ("ss_quantity", "Int"), ("ss_list_price", "Double"), ("ss_coupon_amt", "Double"),
	//			("ss_sales_price", "Double"))
	//		def getDimensions = List("i_item_id", "cd_gender", "cd_marital_status", "cd_education_status",
	//			"p_channel_email", "p_channel_event", "d_year")
	//		def getDimensionFields = List(("i_item_id", "item"), ("cd_gender", "customer_demographics"),
	//			("cd_marital_status", "customer_demographics"), ("cd_education_status", "customer_demographics"),
	//			("p_channel_email", "promotion"), ("p_channel_event", "promotion"), ("d_year", "date_dim"))
	//		def getMeasures = List(("ss_quantity","count"), ("ss_quantity","sum"),
	//				("ss_list_price","count"), ("ss_list_price","sum"), 
	//				("ss_coupon_amt","count"), ("ss_coupon_amt","sum"),
	//				("ss_sales_price","count"), ("ss_sales_price","sum"))
	//
	//	}
	//	def getCubeDimensions(cube: String): Option[List[String]] = cube match {
	//		case "hive" => Some(hive.getDimensions)
	//		case _ => None
	//
	//	}
	//	def getCubeDimensionFields(cube: String): Option[List[(String, String)]] = cube match {
	//		case "hive" => Some(hive.getDimensionFields)
	//		case _ => None
	//
	//	}
	//	def getCubeQery(cube: String, query: String): Map[String, List[Any]] = cube match {
	//		case "hive" => query match {
	//			case "query1" =>
	//				Map("row" -> List("i_item_id"),
	//					"column" -> List(),
	//					"cell" -> List(("ss_quantity", HBase.Averate), ("ss_list_price", HBase.Averate),
	//						("ss_coupon_amt", HBase.Averate), ("ss_sales_price", HBase.Averate)),
	//
	//					"where" -> List(List(("=", "cd_gender", "[GEN]"), ("=", "cd_marital_status", "[MS]"),
	//						("=", "cd_education_status", "[ES]"), ("=", "p_channel_email", "N"), ("=", "d_year", "[YEAR]")),
	//						List(("=", "cd_gender", "[GEN]"), ("=", "cd_marital_status", "[MS]"),
	//							("=", "cd_education_status", "[ES]"), ("=", "p_channel_event", "N"), ("=", "d_year", "[YEAR]"))),
	//
	//					"left" -> List("i_item_id")
	//				//where has two level: or level, and level
	//
	//				)
	//		}
	//	}
	//	def getWhereDimList(where: Option[List[Any]]): List[String] = {
	//		val result = new ArrayBuffer[String]
	//		for (portion <- where; orItem <- portion) {
	//			orItem.asInstanceOf[List[Any]].foreach(_ match {
	//				case (op: String, left: String, right: String) =>
	//					result += left
	//			})
	//		}
	//		result.toList
	//
	//	}
	/*
	def getSql="""select i_item_id, ss_quantity, ss_list_price, ss_coupon_amt, 
        ss_sales_price,cd_gender,cd_marital_status,cd_education_status,
		p_channel_email,p_channel_event,d_year
		from  store_sales join date_dim on (store_sales.ss_sold_date_sk = date_dim.d_date_sk) 
       			join item on (store_sales.ss_item_sk = item.i_item_sk) 
       			join customer_demographics on (store_sales.ss_cdemo_sk = customer_demographics.cd_demo_sk) 
       			join promotion on (store_sales.ss_promo_sk = promotion.p_promo_sk)
        
		"""
	def getDimensions=List("i_item_id","cd_gender","cd_marital_status","cd_education_status",
		"p_channel_email","p_channel_event","d_year")
	def getMeasures=List(("ss_quantity","average"), ("ss_list_price","average"), ("ss_coupon_amt","average"), 
        ("ss_sales_price","average"))
         */

}