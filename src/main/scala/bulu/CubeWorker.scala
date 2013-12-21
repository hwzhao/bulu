package bulu
import language.postfixOps
import scala.concurrent.duration._
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.RootActorPath
import akka.actor.Terminated
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.CurrentClusterState
import akka.cluster.ClusterEvent.MemberUp
import akka.cluster.Member
import akka.cluster.MemberStatus
import akka.pattern.ask
import akka.util.Timeout
import akka.actor.ActorLogging
import bulu.util.ConfigHelper
import java.sql.DriverManager
import bulu.core.Field
import bulu.core.DbHelper
import scala.collection.mutable.ArrayBuffer
import bulu.actor.build._
import bulu.util._
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.util.Bytes
import bulu.util.QueryPart
import bulu.actor.query._
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._
import scala.collection.mutable;
import bulu.core.CuboidPartition

//#messages
case class TransformationJob( text : String )
case class TransformationResult( text : String )
case class JobFailed( reason : String, job : TransformationJob )
case object BackendRegistration
case object Stop

//#messages

object CubeWorker extends App {
	// Override the configuration of the port
	// when specified as program argument
	if ( args.nonEmpty ) System.setProperty( "akka.remote.netty.port", args( 0 ) )

	val system = ActorSystem( "ClusterSystem" )
	system.actorOf( Props[CubeWorker], name = "worker" )
}

class CubeWorker extends Actor with ActorLogging {

	val cluster = Cluster( context.system )
	val cubeDispatchers = scala.collection.mutable.Map.empty[String, Map[Int, ActorRef]]
	val queryDispatchers = scala.collection.mutable.Map.empty[String, Map[Int, ActorRef]]
	val mappers = scala.collection.mutable.Map.empty[String, Map[Option[String], Int]]
	val cubePartitions = mutable.Map.empty[String, Field]
	var localSinker : ActorRef = null
	val dispatchRecordcount = mutable.Map.empty[ActorRef, Int]
	var recordCount = 0
	var expectedRecordCount = Int.MaxValue
	def isRecordFinished = recordCount == expectedRecordCount
	log.info( "java runtime parameter: %s" format SystemHelper.getRuntimeParameters.mkString( "," ) )
	// subscribe to cluster changes, MemberUp
	// re-subscribe when restart
	override def preStart() : Unit = cluster.subscribe( self, classOf[MemberUp] )
	override def postStop() : Unit = cluster.unsubscribe( self )

	def receive = {
		case TransformationJob( text ) ⇒ sender ! TransformationResult( text.toUpperCase )
		case state : CurrentClusterState ⇒
			state.members.filter( _.status == MemberStatus.Up ) foreach register
		case MemberUp( m ) ⇒ register( m )
		case Stop =>
			log.info( "exit worker: %s" format self )
			context.system.shutdown
		case FetchDimension( cube, field, sinker ) =>
			log.info( "fetch Dimension of cube (%s) field (%s)".format( cube, field ) )
			val driver = ConfigHelper.getDriver( cube )
			val url = ConfigHelper.getUrl( cube )
			try {
				Class.forName( driver );
			}
			catch {
				case e : ClassNotFoundException =>
					log.error( e, "" );
			}
			val con = DriverManager.getConnection( url, "", "" )
			val stmt = con.createStatement()
			val sql = "Select Distinct %s From %s ".format( field.column, field.table )
			val res = stmt.executeQuery( sql )
			var result = new ArrayBuffer[Option[String]]
			while ( res.next() ) {
				result += unify( DbHelper.getFunc( res, field.dataType )( field.column ) )
			}
			res.close()
			stmt.close()
			con.close()
			sinker ! MemberList( cube, field, result.toList.sorted )
		case rb : RawDataBegan =>
			expectedRecordCount = Int.MaxValue
			if ( !cubeDispatchers.contains( rb.cube ) ) {
				val dims = HBase.loadDimVK( rb.cube )
				val count = ConfigHelper.actorsForWorker
				val mapper = mutable.Map.empty[Option[String], Int]
				for ( i <- 0 until rb.partList.size ) {
					val mod = i % count
					mapper += rb.partList( i ) -> ( mod )
				}
				mappers += rb.cube -> mapper.toMap
				val actors = mutable.Map.empty[Int, ActorRef]
				for ( i <- 0 until count ) {
					val newOne = context.actorOf( Props( new CubeDispatcher( rb.cube, rb.sinker, dims, rb.workerId, i ) ), name = "cubeDispatcher-%s@%s-%sof%s".format( rb.cube, rb.workerId, i, count ) )
					dispatchRecordcount += newOne -> 0
					actors += i -> newOne
				}
				cubeDispatchers += rb.cube -> actors.toMap
				cubePartitions += rb.cube -> rb.partitionField
			}
			//			for ((id, actor) <- cubeDispatchers(rb.cube)) {
			//				actor ! rb
			//			}
			log.info( "Raw data begin worker (%s)@(%s) partition (%s) with count %s".format( self, rb.workerId, rb.partitionField,rb.partList.size) )
		case RecordList( cube, recList ) =>
			recordCount+=recList.size
			log.debug( "worker (%s) get cube (%s) record list (%s)".format( self, cube, recList ) )
			for ( rec <- recList ) {
				val cubeActor : ActorRef = cubeDispatchers( cube )( mappers( cube )( rec( cubePartitions( cube ).column ).asInstanceOf[Option[String]] ) )
				log.debug( "send record (%s) to cube dispatcher(%s)".format( rec, cubeActor ) )
				cubeActor ! Record( rec )
				dispatchRecordcount( cubeActor ) += 1
			}
			if ( isRecordFinished ) self  ! RawDataFinished( cube, null, recordCount )
		case RawDataFinished( cube, sinker, count ) =>
			expectedRecordCount = count
			log.info( "Raw data finish woker (%s) get (%s)".format( self, cube ) )
			if ( isRecordFinished ) {
				for ( ( id, actor ) <- cubeDispatchers( cube ) ) {
					actor ! RawDataDispatchFinished( dispatchRecordcount( actor ) )
				}
			}
		//		case CachedCube(cubePart, aggs, workerId, workerCount) =>
		//			
		//			val dispatchers =getQeuryDispatchers(cubePart)
		//				
		//			dispatchers(workerId) ! CachedAggs(aggs)
		case QueryPart( cube, cubePart, query, sinker, cuboidMask, filters ) =>
			//val key = getKey(cube, cubePart)
			log.info( "worker (%s) get query part(%s) of query (%s) of cube (%s)".format( self, cubePart, query, cube ) )
			val dispatchers = getQueryDispatchers( cubePart )
			localSinker = context.actorOf( Props( new LocalAggSinker( cube, ConfigHelper.actorsForWorker, sinker ) ) )
			for ( ( id, dispatcher ) <- dispatchers ) dispatcher ! QueryReply( query, localSinker, cuboidMask, filters )
		case CachePart( cube, cubePart, query, sinker ) =>
			//val key = getKey(cube, cubePart)
			log.info( "worker (%s) get cache part(%s) of query (%s) of cube (%s)".format( self, cubePart, query, cube ) )
			val dispatchers = getQueryDispatchers( cubePart )

			for ( ( id, dispatcher ) <- dispatchers ) dispatcher ! CacheReply( query, sinker )
		//		case FetchedCell(cube, query, key, cell, dispatchIndex) =>
		//			val dispatchers = queryDispatchers(cube)
		//			val dispatcher = dispatchers(dispatchIndex)
		//			dispatcher ! BaseCell(query, key, cell)
		//		case sf: SendFinished =>
		//			val dispatchers = queryDispatchers(sf.cube)
		//			for ((key,disp) <- dispatchers) {
		//				disp ! sf
		//			}
	}
	def getQueryDispatchers( cubePart : CuboidPartition ) = {
		if ( queryDispatchers.contains( cubePart.cube ) )
			queryDispatchers( cubePart.cube )
		else {
			val count = ConfigHelper.actorsForWorker
			val actors = mutable.Map.empty[Int, ActorRef]
			for ( i <- 0 until count ) {
				val newOne = context.actorOf( Props( new QueryDispatcher( cubePart, i, count ) ), name = getKey( cubePart, i, count ) )
				actors += i -> newOne
			}
			queryDispatchers += cubePart.cube -> actors.toMap
			actors.toMap
		}
	}
	def getKey( cubePart : CuboidPartition, workerIndex : Int, workerCount : Int ) = "QueryDispatcher-%s-%sof%sParts-%sof%sWorkers".format( cubePart.cube, cubePart.partIndex, cubePart.partCount, workerIndex, workerCount )

	def unify( member : Any ) : Option[String] = {
		if ( member == null ) None
		else Some( member.toString )
	}
	// try to register to all nodes, even though there
	// might not be any frontend on all nodes
	def register( member : Member ) : Unit =
		context.actorFor( RootActorPath( member.address ) / "user" / "master" ) !
			BackendRegistration
}