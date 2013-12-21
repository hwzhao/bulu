package bulu.core

import java.sql.ResultSet

object DbHelper {
def getFunc(res: ResultSet, dataType: String) = {
		dataType match {
			case "String" => res.getString(_: String)
			case "Int" => res.getInt(_: String)
			case "Double" => res.getDouble(_: String)
			case "Float" => res.getFloat(_: String)
			case "Long" => res.getLong(_: String)
			case "Boolean" => res.getBoolean(_: String)
			case "BigDecimal" => res.getBigDecimal(_: String)
		}
	}
}