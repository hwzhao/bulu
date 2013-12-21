package bulu.util

import java.lang.management.ManagementFactory
import bulu.core.BitKey

object SystemHelper {
	def getRuntimeParameters={
		val bean = ManagementFactory.getRuntimeMXBean();
		bean.getInputArguments();
	}
		def getLength(mask:BitKey, start:Int):Int=	mask.cardinality()
	def getStartBit(mask: BitKey): Int = {
		for (i <- 0 until mask.cardinality() ) {
			if (mask.get(i))
				return i
		}
		throw new Exception()
	}
	def getBinaryString(mask: BitKey, start:Int, length:Int): String = {
		val sb=new StringBuilder()
		for (i <- 0 until length ) {
			if (mask.get(i+start))
				sb.insert(0, '1')
			else
				sb.insert(0, '0')
		}
		sb.toString
	}
}