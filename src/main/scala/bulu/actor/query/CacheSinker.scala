package bulu.actor.query

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorLogging
import bulu.util._

class CacheSinker ( query:Query, count : Int, reply : ActorRef ) extends Actor with ActorLogging {
	var left = count
	
	def receive : Receive = {
		case  CachePartFinished(cube, size,workerIndex, dispatcherIndex) =>
			left-=1
			log.info("receive query (%s) cached %s cell on worker %s, dispatcher %s, left %s".
					format(query, size, workerIndex, dispatcherIndex, left))
			
			if(left==0)
				reply ! AllCacheFinished( query)
		case _ =>
			throw new Exception("illegal message")
	}
}