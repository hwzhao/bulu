package bulu.util

import bulu.core.MeasureType
import bulu.core.BitKey

import java.util.BitSet
import org.apache.hadoop.hbase.util.Bytes


object Convert {
	def toByteArray(bitkey: BitKey): Array[Byte] = {
		val bits = bitkey.toBitSet()
		val bytes = new Array[Byte](bits.length() / 8 + 1);
		for (i <- 0 until bits.length) {
			if (bits.get(i)) {
				bytes(bytes.length - i / 8 - 1) = (bytes(bytes.length - i / 8 - 1) | (((1).<<(i % 8)))).toByte
			}
		}
		bytes
	}

	def fromByteArray(bytes: Array[Byte]): BitKey = {
		val bits = new BitSet
		for (i <- 0 until bytes.length * 8) {
			if ((bytes(bytes.length - (i / 8) - 1) & (1 << (i % 8))) > 0) {
				bits.set(i);
			}
		}
		BitKey.Factory.makeBitKey(bits)

	}
	def measureName2Qualifier(name: (String, MeasureType.MeasureType)): Array[Byte] = {
		Bytes.toBytes(name._1 + "@" + name._2.toString())
	}
	def qualifier2MeasureName(qualifier: Array[Byte]): (String, MeasureType.MeasureType) = {

		val names = Bytes.toString(qualifier).split("@")
		if (names.length == 2)
			return (names(0), MeasureType.withName(names(1)))
		throw new Exception("error Qualifier Name[%s]" format qualifier)

	}

	def toLong(value: Any): Long = {
		if (value == null)
			throw new IllegalArgumentException()
		if (value.isInstanceOf[BigDecimal])
			return (value.asInstanceOf[BigDecimal] * 100).toLong
		if (value.isInstanceOf[Int])
			return value.asInstanceOf[Int] * 100
		if (value.isInstanceOf[Long])
			return value.asInstanceOf[Long] * 100
		if (value.isInstanceOf[Float])
			return (value.asInstanceOf[Float].toDouble * 100).toLong
		if (value.isInstanceOf[Double])
			return (value.asInstanceOf[Double] * 100).toLong

		return value.toString.toLong * 100
	}
	def fromLong(value: Long): BigDecimal = {

		return BigDecimal(value) / 100
	}
}