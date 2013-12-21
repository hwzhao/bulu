package bulu.core

case class Field(table:String, column:String, dataType:String, cubeType:CubeMetaType.CubeMetaType)
case class CuboidPartition(cube:String,  partIndex:Int, partCount:Int)
class MetaData {

}