package bulu
import akka.actor.{ Actor, Props, ActorLogging, ActorSystem }
import akka.cluster.Cluster
import akka.actor.Terminated
import akka.actor.ActorRef
import akka.util.Timeout
import bulu.util._
import bulu.actor.{ Builder, Executer }
import org.slf4j.LoggerFactory
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.MasterNotRunningException
import scala.collection.JavaConversions._
import scala.concurrent.duration._
import java.io.File

object CubeMaster extends App {
	// Override the configuration of the port
	// when specified as program argument
	if ( args.nonEmpty ) System.setProperty( "akka.remote.netty.port", args( 0 ) )
	def logger = LoggerFactory.getLogger( "bulu.Master" )
	val system = ActorSystem( "ClusterSystem" )
	logger.info( "java runtime parameter: %s" format SystemHelper.getRuntimeParameters.mkString( "," ) )
	logger.info( "initial system" )
	val frontend = system.actorOf( Props[CubeMaster], name = "master" )
	//	val allTasks = Map("stop" -> stopper, "build" -> builder, "execute" -> executer)

	try {
		HBaseAdmin.checkHBaseAvailable( HBase.getConf() );
		logger.info( "system started..." )
		for ( ln <- io.Source.stdin.getLines ) {
			frontend ! Command( ln )

		}
	}
	catch {
		case e : MasterNotRunningException =>
			logger.error( "HBase is not running." )
			system.shutdown
	}

}
case class Command( cmd : String )
class CubeMaster extends Actor with ActorLogging {
	import scala.collection.mutable
	val builders = mutable.Map.empty[String, ActorRef] //context.actorOf(Props[Builder], name = "builder")
	val executers = scala.collection.mutable.Map.empty[String, ActorRef]
	var workers = IndexedSeq.empty[ActorRef]
	var jobCounter = 0
	val BuildExp = """build\s(\S+)\s(\S+)""".r
	val ExecuteExp = """execute\s(\S+)\s(\S+)\s([\s\S]+)""".r

	def receive = {
		case Command( cmd ) =>
			cmd match {
				case BuildExp( cube, category ) =>
					category match {
						case "init" =>
							val key = getKey( cube, workers )
							val builder =
								if ( builders.contains( key ) )
									builders( key )
								else {
									val newOne = context.actorOf( Props( new Builder( cube, workers ) ), name = "builder-" + key )
									builders += key -> newOne
									newOne
								}
							builder ! BuildStart //TODO: support multi-source
							log.info( "begin to build cube (%s) with %s workers".format( cube, workers.size ) )
						case _ => notifyNotKnow
					}

				case ExecuteExp( cube, queryName, params ) =>

					//	 params match {
					//						case "result" =>
					//							val future = (executor ? QueryResult(cube, queryName)).mapTo[String]
					//							val result = Await.result(future, timeout.duration)
					//							new CamelMessage(" %s " format result, Map.empty[String, Object])

					//	case _ =>
					if ( queryName == "All1g" ) {
						self ! Command( "execute hive1g query7 GEN=M&MS=S&ES=College&YEAR=2000" )
						self ! Command( "execute hive1g query42 YEAR=2000&MONTH=7" )
						self ! Command( "execute hive1g query52 YEAR=2000&MONTH=7" )
						self ! Command( "execute hive1g query55 YEAR=2000&MONTH=7&MANAGER=21" )

					}
					else if ( queryName == "All10g" ) {
						self ! Command( "execute hive10g query710 GEN=M&MS=S&ES=College&YEAR=2000" )
						self ! Command( "execute hive10g query4210 YEAR=2000&MONTH=7" )
						self ! Command( "execute hive10g query5210 YEAR=2000&MONTH=7" )
						self ! Command( "execute hive10g query5510 YEAR=2000&MONTH=7&MANAGER=21" )
					}
					else if ( queryName == "All100g" ) {
						self ! Command( "execute hive100g query7100 GEN=M&MS=S&ES=College&YEAR=2000" )
						self ! Command( "execute hive100g query42100 YEAR=2000&MONTH=7" )
						self ! Command( "execute hive100g query52100 YEAR=2000&MONTH=7" )
						self ! Command( "execute hive100g query55100 YEAR=2000&MONTH=7&MANAGER=21" )
					}
					else {
						val key = getKey( cube, workers )

						val executer =
							if ( executers.contains( key ) )
								executers( key )
							else {
								val newOne = context.actorOf( Props( new Executer( cube, workers, self ) ) ) //, name = "executer" + key)
								executers += key -> newOne
								newOne
							}

						val query = Query( queryName, getParams( params ), workers )
						executer ! query
					}
				//	}

				case "exit" =>

					workers foreach ( _ ! Stop )
					//					import akka.pattern.gracefulStop
					//					import scala.concurrent.Await
					//					try {
					//						val buildStopped: Future[Boolean] = gracefulStop(builder, 5 seconds)(context.system)
					//						Await.result(buildStopped, 6 seconds)
					//						//						val executeStopped = gracefulStop( executor, 5 seconds )( context.system )
					//						//						Await.result( executeStopped, 6 seconds )
					//						// the actor has been stopped
					//					}
					//					catch {
					//						// the actor wasn’t stopped within 5 seconds
					//						case e: akka.pattern.AskTimeoutException =>
					//							log.warning("to stop master throws exception: ", e)
					//					}
					val cluster = Cluster( context.system )
					cluster.system.shutdown

			}
		//case QueryResult(query,result)=> 

		case job : TransformationJob if workers.isEmpty ⇒
			sender ! JobFailed( "Service unavailable, try again later", job )

		case job : TransformationJob ⇒
			jobCounter += 1
			workers( jobCounter % workers.size ) forward job

		case BackendRegistration if !workers.contains( sender ) ⇒
			context watch sender
			workers = workers :+ sender

		case Terminated( a ) ⇒
			workers = workers.filterNot( _ == a )
	}

	def notifyNotKnow = { println( "the command is not known" ) }
	def getKey( cube : String, workers : Seq[ActorRef] ) = cube + "-" + workers.size

	def getParams( params : String ) : Map[String, String] = {
		val result = scala.collection.mutable.Map.empty[String, String]
		for ( pair <- params.split( "&" ) ) {
			val arr = pair.split( "=" )
			if ( arr.length == 2 ) {
				result += ( arr( 0 ) -> arr( 1 ) )
			}
		}
		result.toMap
	}
}
