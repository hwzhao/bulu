package bulu.actor.build

import akka.actor.ActorRef
import akka.actor.Actor
import akka.actor.ActorLogging
import bulu.util._

class CubeSinker(count: Int, reply: ActorRef) extends Actor with ActorLogging {
	var left = count * ConfigHelper.actorsForWorker
	var cells = 0
	def receive: Receive = {
		case CubePartFinished(cube, condition, cellCount) =>
			log.info("Cube %s@%s is finished record, %s".format(cube, condition,cellCount))
			cells += cellCount
			left -= 1
			if (left == 0) {
				reply ! CubeFinshed(cube, cells)
			}

	}
}