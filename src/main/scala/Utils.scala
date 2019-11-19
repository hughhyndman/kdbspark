/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.types._
import scala.collection.JavaConversions.mapAsScalaMap

import java.sql.{Timestamp => JTimestamp, Date => JDate}
import java.util.UUID
import java.lang.Math.{max, min}
import java.lang.Boolean
import java.net.InetAddress

import org.apache.log4j.Level
import org.apache.log4j.Logger

import org.apache.spark.sql.sources.Filter

/*
 * Singleton object for general utilities
 */
object Util {
  /* Set Log4J logging level from options setting */
  def setLevel(options: DataSourceOptions, log: Logger): Unit = {
    val level = options.get("logLevel").orElse("warn") match {
      case "debug" => Level.DEBUG
      case "trace" => Level.TRACE
      case "warn" => Level.WARN
      case "error" => Level.ERROR
      case _ => Level.WARN
    }
    log.setLevel(level)
  }
}

/*
 * Singleton object calling kdb+ functions
 */
object Kdb {
  def schema(options: java.util.Map[String,String]): Object = {
    val hostind = 0 // Use first host to get schema
    val res = call(hostind, options, optionsToDict(options, Map[String,Object]()), null)
    if (!res.isInstanceOf[c.Flip])
      throw new Exception("Read call must return an unkeyed table with query results") 
    res
  }
  
  def query(options: java.util.Map[String,String], filters: Array[Object], schema: StructType): Object = {
    val hostind = optGetInt(options, "partitionid", 0)
    
    //
    // Additional options to provide to kdb+
    //
    val objmap = Map[String, Object](
        "filters" -> filters, 
        "columns" -> schemaColumns(schema),
        "executor" -> InetAddress.getLocalHost.toString
    )
    
    val res = call(hostind, options, optionsToDict(options, objmap), null)
    if (!res.isInstanceOf[c.Flip])
      throw new Exception("Read call must return an unkeyed table with meta information")
    res
  }
  
  def write(options: java.util.Map[String,String], schema: StructType, coldata: Array[Object]): Unit = {
 //TODO: Doublecheck (understand) range of values for partionId coming from Spark
    val hostind = optGetInt(options, "partitionid", 0) 
    val flip = new c.Flip(new c.Dict(schemaColumns(schema), coldata))
    val res = call(hostind, options, optionsToDict(options, Map[String,Object]()), flip)
  }
  
  def call(hostind: Int, options: java.util.Map[String,String], arg1: Object, arg2: Object): Object = {    
    /*
     * If a list of kdb+ host names is provided, use the hostind as the
     * index. A list of ports, parallel to the hosts may be provided.
     */
    val hosts = optGet(options, "host", "localhost").split(";") 
    val ports = optGet(options, "port", "5000").split(";")
  
    val pind = max(0, hostind) 
    val host = hosts(pind % hosts.length) // Round robin if less hosts are provided
    val port = ports(pind % ports.length).toInt
    
    val userpass = optGet(options, "userpass", "")
    val useTLS = optGetBoolean(options, "useTLS", false)

    val qexpr = optGet(options, "q", "")
    val fexpr = optGet(options, "function", "")
   
    /* Build a request object to accommodate 1-3 arguments to k() */
    val req = if (qexpr.length > 0) 
      qexpr.toCharArray
    else if (arg2 == null)
      Array(fexpr.toCharArray, arg1)
    else
      Array(fexpr.toCharArray, arg1, arg2)      

    val platformTarget = optGet(options, "platformtarget", "")
    if (platformTarget.length > 0) {
 //TODO: Brendan's platform code, wrapping <req>     
    }
      
    val con = new c(host, port, userpass, useTLS)
    val res = con.k(req)        
    con.close()  
    res
  }  
  
  def optGet(options: java.util.Map[String,String], key: String, default: String): String = 
    if (options.containsKey(key)) options.get(key) else default    
  
  def optGetBoolean(options: java.util.Map[String,String], key: String, default: Boolean): Boolean =
    if (options.containsKey(key)) Boolean.parseBoolean(options.get(key)) else default      

  def optGetInt(options: java.util.Map[String,String], key: String, default: Int): Int =
    if (options.containsKey(key)) options.get(key).toInt else default
  
  /* Convert options and additional objects to a kdb+ dictionary */  
  private def optionsToDict(optionmap: java.util.Map[String,String], objmap: Map[String, Object]): c.Dict = {
    val n = optionmap.size + (if (objmap != null) objmap.size else 0) 
    var keys = new Array[String](n)
    var vals = new Array[Object](n)
    var i = 0
       
    /* Append options */
    for ((k,v) <- optionmap) {
      /* convert all keys to lower case and dictionary values to a convenient datatype */
      keys(i) = k.toLowerCase      
      vals(i) = keys(i) match {
        case "loglevel" | "writeaction" => v
        case "partitionid" | "numpartitions" | "batchingsize" | "batchcount" => 
            v.toLong.asInstanceOf[Object]
        case "useTLS" => v.toBoolean.asInstanceOf[Object]
        case _ => v.toCharArray.asInstanceOf[Object]
      }
      i += 1
    }
    
    /* Append additional objects */
    for ((k,v) <- objmap) {
      keys(i) = new String(k); vals(i) = v
      i += 1     
    }
    
    new c.Dict(keys, vals)
  }
  
  /* Given a schema, return a list of column name strings */
  private def schemaColumns(schema: StructType): Array[String] = {
    var res = new Array[String](schema.length)
    for (i <- 0 until schema.length)
      res(i) = schema(i).name
    res
  }  
  
  /*
   * Constants to help with null checks
   */
  val LongNull = Long.MinValue
  val IntNull = Integer.MIN_VALUE
  val ShortNull = Short.MinValue
  val UUIDNull = new UUID(0, 0)
  val FloatNull = Float.NaN
  val DoubleNull = Double.NaN
  val StringNull = "".toCharArray.asInstanceOf[Object]
  val TimestampNull = new JTimestamp(Long.MinValue)
  val DateNull = new JDate(Long.MinValue)  

  /*
   * Custom data types to support kdb+ types. Values in the arrays cannot contain nulls
   */
  val ByteArrayType = DataTypes.createArrayType(ByteType, false)
  val ShortArrayType = DataTypes.createArrayType(ShortType, false)  
  val IntegerArrayType = DataTypes.createArrayType(IntegerType, false)
  val LongArrayType = DataTypes.createArrayType(LongType, false)
  val FloatArrayType = DataTypes.createArrayType(FloatType, false)
  val DoubleArrayType = DataTypes.createArrayType(DoubleType, false)
  val TimestampArrayType = DataTypes.createArrayType(TimestampType, false)
  val DateArrayType = DataTypes.createArrayType(DateType, false)
  
  def mapDataType(from: Char): DataType = k2s(from)  

  private val k2s = Map(
		'b' -> DataTypes.BooleanType,
		'g' -> DataTypes.StringType,  
		'x' -> DataTypes.ByteType,
		'X' -> ByteArrayType,
		'h' -> DataTypes.ShortType,
		'H' -> ShortArrayType,
		'i' -> DataTypes.IntegerType,
		'I' -> IntegerArrayType,
		'j' -> DataTypes.LongType,
		'J' -> LongArrayType,
		'e' -> DataTypes.FloatType,
		'E' -> FloatArrayType,
		'f' -> DataTypes.DoubleType,
		'F' -> DoubleArrayType,
		'c' -> DataTypes.StringType,
		'C' -> DataTypes.StringType,		
		's' -> DataTypes.StringType,
		'p' -> DataTypes.TimestampType,
		'P' -> TimestampArrayType,
		'm' -> DataTypes.DateType,
		'd' -> DataTypes.DateType,
		'D' -> DateArrayType,	
    'z' -> DataTypes.TimestampType, // Datetime
    'n' -> DataTypes.TimestampType, // Timespan
    'u' -> DataTypes.IntegerType, // Minute
    'v' -> DataTypes.IntegerType, // Second
    't' -> DataTypes.TimestampType // Time  
  )
}