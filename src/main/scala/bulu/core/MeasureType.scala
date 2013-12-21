package bulu.core

object MeasureType extends Enumeration {
	type MeasureType=Value
	val Count=Value("cnt")
	val Summary=Value("sum")
	val Average=Value("avg")
	val SquarSummary=Value("ssm")
	val Maximum=Value("max")
}