package bulu.actor.build
import akka.actor.Actor
import bulu.util._
import akka.actor.ActorLogging
import org.apache.hadoop.hbase.util.Bytes
import java.sql.DriverManager
import java.sql.ResultSet
import akka.actor.actorRef2Scala
import akka.actor.ActorRef
import bulu.core.Field
import bulu.core.BitKey



class DimensionSinker( count : Int, reply : ActorRef, workers:Seq[ActorRef] ) extends Actor with ActorLogging {
	var left = count
	var partitionList = List.empty[Option[String]]
	var partitionField:Field=null
	var keyCursor = 0
	var maxList=0
	def receive : Receive = {
		case MemberList( cube, field, list ) =>
			log.info("receive cube (%s) dimension (%s) result, count=(%s)".format(cube, field, list.size))
			left = left - 1
			val length = Integer.toBinaryString( list.size ).length();

			val maskTable = new HBase( HBase.getTableName( cube, TableCategory.Mask ) )
			val mask = BitKey.Factory.makeBitKey( keyCursor + length, false );
			for ( i <- 0 until length ) {
				mask.set( keyCursor + i )
			}
			saveMask( maskTable, field.column, mask )
			for ( index <- 0 until list.size ) {
				val key = BitKey.Factory.makeBitKey( keyCursor + length, false );
				setKey( index + 1, key, keyCursor )
				saveKV( cube, field.column, key,  list( index ) )
			}
			if(list.size>maxList){
				partitionField=field
				partitionList=list
				maxList=list.size
			}
			keyCursor+=length
			if(left==0)
				reply ! DimensionFinshed( cube ,partitionField,partitionList, workers)
		case _ =>
			throw new Exception("illegal message")
	}
	def setKey( index : Int, key : BitKey, base : Int ) {
		val length = Integer.toBinaryString( index ).length();
		for ( i <- 0 until length ) {
			if ( ( index >> i & 1 ) == 1 ) {
				key.set( i+base )
			}
		}
	}
	
	def getFunc( res : ResultSet, dataType : String ) = {
		dataType match {
			case "String" => res.getString( _ : String )
			case "Int" => res.getInt( _ : String )
			case "Double" => res.getDouble( _ : String )
			case "Float" => res.getFloat( _ : String )
			case "Long" => res.getLong( _ : String )
			case "Boolean" => res.getBoolean( _ : String )
			case "BigDecimal" => res.getBigDecimal( _ : String )
		}
	}
	def saveCursor( maskTable : HBase, cursor : Int ) {
		maskTable.put( Bytes.toBytes( HBase.MaxKeyCursor ), Bytes.toBytes( HBase.DefaultFamily ),
			Bytes.toBytes( HBase.MaxKeyCursor ), Bytes.toBytes( cursor.toLong ) )
	}
	def getMask( maskTable : HBase, dim : String ) : Option[BitKey] = {
		val res = maskTable.get( Bytes.toBytes( HBase.DefaultMaskRowId ), Bytes.toBytes( HBase.DefaultFamily ), Bytes.toBytes( dim ) )
		if ( res == null ) None else Some( Convert.fromByteArray( res ) )
	}
	def saveMask( maskTable : HBase, dim : String, maskKey : BitKey ) {
		maskTable.put( Bytes.toBytes( HBase.DefaultMaskRowId ), Bytes.toBytes( HBase.DefaultFamily ),
			Bytes.toBytes( dim ), Convert.toByteArray( maskKey ) )
	}

	def saveKV( cube : String, dimName : String, key : BitKey, value : Option[Any] ) {
		val dimKVTable = new HBase( HBase.getTableName( cube, TableCategory.DimKV ) )
		val dimVKTable = new HBase( HBase.getTableName( cube, TableCategory.DimVK ) )
		dimKVTable.put( Bytes.toBytes( dimName ), Bytes.toBytes( HBase.DefaultFamily ),
			Convert.toByteArray( key ), Bytes.toBytes( value.toString ) )
		dimVKTable.put( Bytes.toBytes( dimName ), Bytes.toBytes( HBase.DefaultFamily ),
			Bytes.toBytes( value.toString ), Convert.toByteArray( key ) )
		dimKVTable.close
		dimVKTable.close

	}
	

}