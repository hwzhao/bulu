package bule.actor.query
import akka.actor.Actor
import bulu.util._
import akka.actor.ActorLogging
import akka.actor.actorRef2Scala
class CuboidBuilder(cube:String)  extends Actor with ActorLogging {
	def receive : Receive = {
		case BaseCell=>
			//sender ! getReady( query, key )
		case BaseFinished =>
	}
//	def getReady( query : Query, key : BitKey ) : CuboidReady =
//		if ( isBaseCuboid( key ) ) { CuboidReady( query, HBase.getTableName( query.cube, TableCategory.CuboidBase ) ) }
//		else {
//			val tableName = HBase.getTableName( query.cube, TableCategory.CuboidBase, key )
//			if ( !HBase.exists( tableName ) ) {
//				//TODO: build other cuboid
//			}
//			CuboidReady( query, tableName )
//		}
//
//	def isBaseCuboid( key : BitKey ) : Boolean = {
//		for ( i <- 0 until key.cardinality ) {
//			if ( !key.get( i ) ) return false
//		}
//		return true
//	}
}