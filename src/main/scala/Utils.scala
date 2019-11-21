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
import java.sql.{Date => JDate, Timestamp => JTimestamp}
import java.util.UUID
import java.lang.Math.max
import java.lang.Boolean
import java.io.IOException

import c.KException

import org.apache.log4j.{Level, Logger}

/*
 * Singleton object for general utilities
 */
object Util {
  /* Set Log4J logging level from options setting */
  def setLevel(options: DataSourceOptions, log: Logger): Unit = {
    val level = options.get(Opt.LOGLEVEL).orElse("") match {
      case "debug" => Level.DEBUG
      case "trace" => Level.TRACE
      case "warn" => Level.WARN
      case "error" => Level.ERROR
      case "" => log.getLevel // Use Log4J setting (or last setting)
    }
    log.setLevel(level)
  }
}

/*
 * Singleton object calling kdb+ functions
 */
object KdbCall {
  @transient lazy val log: Logger = Logger.getLogger("kdb")

  /* If the table option is specified, convert it to a q expression */
  def adjustForTable(options: java.util.Map[String, String], prefix: String): Unit = {
    val tablename = optGet(options, Opt.TABLE, "")
    if (tablename.length > 0)
      options(Opt.QEXPR) = prefix + tablename
  }

  /* Request for the kdb+ schema information required to map the query result to Spark */
  def schema(options: java.util.Map[String, String]): Object = {
    adjustForTable(options, "0!meta ")
    val hostind = 0 // Use the first host to get the schema
    val res = call(hostind, options, optionsToDict(options, Map[String, Object]()), null)
    if (!res.isInstanceOf[c.Flip])
      throw new Exception("Read call must return an unkeyed table with meta information")
    res
  }

  /* Make the kdb+ query */
  def query(options: java.util.Map[String, String], filters: Array[Object], schema: StructType): Object = {
    adjustForTable(options, "0!")

    /* Additional options to provide to kdb+ */
    val objmap = Map[String, Object](
      Opt.FILTERS -> filters,
      Opt.COLUMNS -> schemaColumns(schema)
    )

    val hostind = optGetInt(options, Opt.PARTITIONID, 0)
    val res = call(hostind, options, optionsToDict(options, objmap), null)
    if (!res.isInstanceOf[c.Flip])
      throw new Exception("Read call must return an unkeyed table with query results")
    res
  }

  def write(options: java.util.Map[String, String], schema: StructType, coldata: Array[Object]): Unit = {
    //TODO: Doublecheck (understand) range of values for partionId coming from Spark
    val hostind = optGetInt(options, Opt.PARTITIONID, 0)
    val flip = new c.Flip(new c.Dict(schemaColumns(schema), coldata))
    val res = call(hostind, options, optionsToDict(options, Map[String, Object]()), flip)
  }

  def call(hostind: Int, options: java.util.Map[String, String], arg1: Object, arg2: Object): Object = {
    /*
     * If a list of kdb+ host names is provided, use the hostind as the
     * index. A list of ports, parallel to the hosts may be provided.
     */
    val hosts = optGet(options, Opt.HOST, Opt.HOSTDEF).split(";")
    val ports = optGet(options, Opt.PORT, Opt.PORTDEF).split(";")

    val pind = max(0, hostind)
    val host = hosts(pind % hosts.length) // Round robin if less hosts are provided
    val port = ports(pind % ports.length).toInt

    val userpass = optGet(options, Opt.USERPASS, Opt.USERPASSDEF)
    val useTLS = optGetBoolean(options, Opt.USETLS, Opt.USETLSDEF)

    /*
     * These two options are mutually exclusive Either a function is called (that can return
     * the schema and results, or a q expression is provided (where a schema has to be provided
     * by the Spark caller).
     */
    val qexpr = optGet(options, Opt.QEXPR, "")
    val fexpr = optGet(options, Opt.FUNCTION, "")

    val timeout = optGet(options, Opt.TIMEOUT, Opt.TIMEOUTDEF).toInt

    /* Build a request object to accommodate 1-3 arguments to k() */
    val req = if (qexpr.length > 0)
      qexpr.toCharArray
    else if (arg2 == null)
      Array(fexpr.toCharArray, arg1)
    else
      Array(fexpr.toCharArray, arg1, arg2)


    /* Connect to kdb+ process. Log any exceptions to help the user diagnose issues */
    var con: c = null
    try {
      con = new c(host, port, userpass, useTLS)
    }
    catch {
      case e: KException => {
        log.error(s"Connection failed with kdb+ message: ${e.getMessage()}")
        throw(e)
      }
      case e: IOException => {
        log.error(s"Socket connection failed with message: ${e.getMessage()}")
        throw(e)
      }
    }

    con.s.setSoTimeout(timeout)

    val res = con.k(req)
    con.close()
    res
  }

  def optGet(options: java.util.Map[String, String], key: String, default: String): String =
    if (options.containsKey(key)) options.get(key) else default

  def optGetBoolean(options: java.util.Map[String, String], key: String, default: Boolean): Boolean =
    if (options.containsKey(key)) Boolean.parseBoolean(options.get(key)) else default

  def optGetInt(options: java.util.Map[String, String], key: String, default: Int): Int =
    if (options.containsKey(key)) options.get(key).toInt else default

  /* Convert options and additional objects to a kdb+ dictionary */
  private def optionsToDict(optionmap: java.util.Map[String, String], objmap: Map[String, Object]): c.Dict = {
    val n = optionmap.size + (if (objmap != null) objmap.size else 0)
    val keys = new Array[String](n)
    val vals = new Array[Object](n)
    var i = 0

    /* Append options */
    for ((k, v) <- optionmap) {
      /* convert all keys to lower case and dictionary values to a convenient kdb+ datatype */
      //TODO: Remove unecessary options (e.g., userpass, host, port, etc.)
      keys(i) = k.toLowerCase
      vals(i) = keys(i) match {
        case "loglevel" | "writeaction" => v
        case "partitionid" | "numpartitions" | "batchsize" | "batchcount" =>
          v.toLong.asInstanceOf[Object]
        case "useTLS" => v.toBoolean.asInstanceOf[Object]
        case _ => v.toCharArray.asInstanceOf[Object]
      }
      i += 1
    }

    /* Append additional objects */
    for ((k, v) <- objmap) {
      keys(i) = new String(k)
      vals(i) = v
      i += 1
    }

    new c.Dict(keys, vals) // Return kdb+ dictionary
  }

  /* Given a schema, return a list of column name strings */
  private def schemaColumns(schema: StructType): Array[String] = {
    val res = new Array[String](schema.length)
    for (i <- 0 until schema.length)
      res(i) = schema(i).name
    res
  }
}

/*
 * This object provides constants that help map values datatypes and values to Spark
 */
object Type {
  /*
   * Constants to help with null checks
   */
  val LongNull: Long = Long.MinValue
  val IntNull: Integer = Integer.MIN_VALUE
  val ShortNull: Short = Short.MinValue
  val UUIDNull = new UUID(0, 0)
  val FloatNull: Float = Float.NaN
  val DoubleNull: Double = Double.NaN
  val StringNull: Object = "".toCharArray.asInstanceOf[Object]
  val TimestampNull = new JTimestamp(Long.MinValue)
  val DateNull = new JDate(Long.MinValue)

  /*
   * Custom data types to support kdb+ types. Values in the arrays cannot contain nulls
   */
  val BooleanArrayType: ArrayType = DataTypes.createArrayType(BooleanType, false)
  val ByteArrayType: ArrayType = DataTypes.createArrayType(ByteType, false)
  val ShortArrayType: ArrayType = DataTypes.createArrayType(ShortType, false)
  val IntegerArrayType: ArrayType = DataTypes.createArrayType(IntegerType, false)
  val LongArrayType: ArrayType = DataTypes.createArrayType(LongType, false)
  val FloatArrayType: ArrayType = DataTypes.createArrayType(FloatType, false)
  val DoubleArrayType: ArrayType = DataTypes.createArrayType(DoubleType, false)
  val TimestampArrayType: ArrayType = DataTypes.createArrayType(TimestampType, false)
  val DateArrayType: ArrayType = DataTypes.createArrayType(DateType, false)

  /* Map of single character kdb+ datatypes to Spark datatypes */
  private val k2s = Map(
		'b' -> DataTypes.BooleanType,
    'B' -> BooleanArrayType,
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

  def mapDataType(from: Char): DataType = k2s(from)
}

/*
 * U: Input from Spark user
 * S: Input from Spark itself
 * G: Generated by this driver; sent to kdb+
 */
object Opt {
  val HOST = "host"; val HOSTDEF = "localhost"  // U: host name or list of host names (separated by ;)
  val PORT = "port"; val PORTDEF = "5000" // U: port number or list of port numbers (separated by ;)
  val TIMEOUT = "timeout"; val TIMEOUTDEF = "60" // U: timeout in seconds
  val USERPASS = "userpass"; val USERPASSDEF = "" // U: user ID and password
  val USETLS = "useTLS"; val USETLSDEF = false // U: whether to use TLS

  val NUMPARTITIONS = "numPartitions"; val NUMPARTITIONSDEF = 1 // U: Number of read tasks (partitions) to create
  val PARTITIONID = "partitionid"; val SCHEMAREQ = "-1" // G: Current partition ID for read task instance

  /*
   * The caller can choose ONE of these options to get data from kdb+
   */
  val TABLE = "table" // U: A table name
  val QEXPR = "qexpr" // U: q expression that returns an unkeyed table
  val FUNCTION = "function" // U: A kdb+ function that processes based on options provided

  val PUSHFILTERS = "pushFilters"; val PUSHFILTERSDEF = true // U: Whether kdb+ function support filters
  val FILTERS = "filters" // G: List of filters that Spark wants kdb+ function to use
  val COLUMNS = "columns" // G: List of columns to return from kdb+ function
  val EXECUTOR = "executor" // G: Informational indicating host information about executor running read task

  val JOBID = "jobID" // S: Job ID of write process
  val MODE = "mode" // S: Mode of write process
  val BATCHSIZE = "batchsize"; val BATCHSIZEDEF = 10000 // U: Number of rows to batch in one kdb+ write call
  val WRITEACTION = "writeaction" // G:
      val WRITE = "write"
      val COMMIT = "commit"
      val ABORT = "abort"
  val BATCHCOUNT = "batchcount" // G:

  val LOGLEVEL = "loglevel"
}
