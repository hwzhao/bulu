package bulu.actor.build

import akka.actor.ActorLogging
import akka.actor.Actor
import akka.actor.ActorRef
import bulu.util.CubePartFinished
import bulu.util.CellSaved

class DispatchSinker(cube: String,partition:String,count:Int, reply: ActorRef) extends Actor with ActorLogging {
	var left=count
	def receive: Receive = {
		case CellSaved =>
			left-=1
			if(left==0){
				log.info("end to save aggregating cells %s count for cube (%s) with condition (%s)".format(count, cube, partition))
				reply ! CubePartFinished(cube, partition, count)
				
			}
	}
}