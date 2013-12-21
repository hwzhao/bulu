package bulu.actor.query

import org.scalatest.FunSuite
import scala.collection.JavaConversions._
import bulu.core.BitKey
import bulu.util.SystemHelper
class HBaseTest extends FunSuite {
	test ( "Conver bitkey to int by mask" ){

		val mask = BitKey.Factory.makeBitKey( 10, false )
		mask.set( 3 )
		mask.set( 4 )
		mask.set( 5 )
		mask.set( 6 )

		
		val bitkey = BitKey.Factory.makeBitKey( 10, false )
		bitkey.set( 4 )
		bitkey.set( 5 )

		val start = SystemHelper.getStartBit( mask )
		assert( start == 3 )

		val length = SystemHelper.getLength( mask, start )
		assert( length == 4 )
		val bin = SystemHelper.getBinaryString( bitkey, start, length )
		assert( bin == "0110" )
		val result = Integer.parseInt( bin, 2 )
		assert( result == 6 )
	}
	
	test ( "Conver bitkey to int by mask 2" ){

		val mask = BitKey.Factory.makeBitKey( 28, false )
		mask.set( 12 )
		mask.set( 13 )
		mask.set( 14 )
		mask.set( 15 )
		mask.set( 16 )
mask.set( 17 )
mask.set( 18 )
mask.set( 19 )
mask.set( 20 )
mask.set( 21 )
mask.set( 22 )
mask.set( 23 )
mask.set( 24 )
mask.set( 25 )

		
		val bitkey = BitKey.Factory.makeBitKey( 28, false )
		bitkey.set( 1 )
		bitkey.set( 3 )
		bitkey.set( 4 )
		bitkey.set( 7 )
		bitkey.set( 9 )
		bitkey.set( 10 )
		bitkey.set( 11 )
		bitkey.set( 12 )
		bitkey.set( 13 )
		bitkey.set( 14 )
		bitkey.set( 16 )
		bitkey.set( 17 )
		bitkey.set( 18 )
		bitkey.set( 19 )
		bitkey.set( 20 )
		bitkey.set( 22 )
		bitkey.set( 24 )
		bitkey.set( 26 )
		bitkey.set( 27 )
		
		val start = SystemHelper.getStartBit( mask )
		assert( start == 12 )

		val length = SystemHelper.getLength( mask, start )
		assert( length == 14 )
		val bin = SystemHelper.getBinaryString( bitkey, start, length )
		assert( bin == "01010111110111" )
		val result = Integer.parseInt( bin, 2 )
		assert( result == 5623 )
	}
}

