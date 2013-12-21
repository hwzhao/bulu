package bulu.util

import java.io.File

object ToolHelper {
	def writeToFile(p: String, s: String) {
		val pw = new java.io.PrintWriter(new File(p))
		try {
			pw.write(s)
		}
		finally {
			pw.close()
		}
	}

	def getHostName = java.net.InetAddress.getLocalHost.getHostName

}