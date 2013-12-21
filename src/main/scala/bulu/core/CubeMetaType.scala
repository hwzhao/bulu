package bulu.core



object CubeMetaType extends Enumeration {
	type CubeMetaType=Value
	val Dimension=Value("Dimension")
	val Measure=Value("Measure")
	val Level=Value("Level")
	val Attribute=Value("Attribute")
	val Member=Value("Member")
}
