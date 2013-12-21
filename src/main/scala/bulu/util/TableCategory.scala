package bulu.util

object TableCategory extends Enumeration {
	type TableCategory=Value
	val DimKV=Value("DimKV")
	val DimVK=Value("DimVK")
	val Mask=Value("Mask")
	val CuboidBase=Value("CuboidBase")
//	val CuboidOther=Value("Cuboid")
}
