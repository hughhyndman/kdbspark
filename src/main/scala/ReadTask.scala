/*
 * Copyright [2019] [Hugh Hyndman]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.types._
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.vectorized.ColumnVector
import org.apache.spark.sql.vectorized.ColumnarBatch
  
import org.apache.log4j.Logger

import java.nio.charset.StandardCharsets
import java.sql.{Timestamp => JTimestamp, Date => JDate}
import java.util.UUID
import java.util

/*
 * Class that is instantiated on an executor to read data from kdb+
 */
class ReadTask(
      optionmap: util.Map[String,String],
      filters: Array[Object],
      schema: StructType) 
    extends InputPartition[ColumnarBatch] with InputPartitionReader[ColumnarBatch] {
  @transient lazy val options = new DataSourceOptions(optionmap)
  @transient lazy val log: Logger = Logger.getLogger("kdb")
  Util.setLevel(options, log) 

  var numrows: Integer = -1 // Indicates that query not run yet
  var coldata: Array[Object] = _ // Column data from kdb+ query
  var colnames: Array[String] = _ // Column names from kdb+ query

  var index = 0
  
  override def createPartitionReader(): InputPartitionReader[ColumnarBatch] = this
  
  /*
   * Determine if there is more data to <get>. kdb+ returns all date in one call,
   * so this function is called once to make the initial query, and a second time to
   * indicate that there is no more data
   */
  override def next(): Boolean = {
    log.debug("next()")
    if (numrows == -1) {   
      val obj = KdbCall.query(optionmap, filters, schema)
      
      /*
       * The flip object has an array of column names, and an array of arrays containing column data	
       */
      val flip = obj.asInstanceOf[c.Flip]
      colnames = flip.x
      coldata = flip.y
      numrows = c.n(flip) 
      index = 0
      
      if (log.isDebugEnabled) {
        log.debug("  #rows: " + numrows)
        log.debug("  cols:  " + colnames.mkString(","))
      }
    }
     
    index < numrows
  }
  
  /*
   * Return all data from kdb+ query in one batch
   */
  override def get(): ColumnarBatch = {
    log.debug("get()")
    index = numrows // Indicate end-of-data for subsequent call to next()
   
    /*
     * Place data received from kdb+ into appropriate column vector. Kdb+ may not
     * have pruned the columns and provided more data, so we want to make sure that
     * we select only what we need from the result
     */
    val acv = new Array[ColumnVector](schema.length)
    for (colind <- 0 until schema.length) { 
      val ind = colnames.indexOf(schema(colind).name)
      if (ind == -1)
        throw new Exception(s"Missing column '${schema(colind).name}' from kdb+ query")

      acv(colind) = populateColumn(schema(colind), numrows, coldata(ind))
      coldata(ind) = null // Free space; we don't need this now
    }    
    val batch = new ColumnarBatch(acv)
    
    batch.setNumRows(numrows)
    batch
  }
  
  /*
   * Populate a Spark data frame column with kdb+ column data <cd>, which has schema
   * properties defined in <sf> 
   */
  private def populateColumn(sf: StructField, numrows: Int, cd: Object): OnHeapColumnVector = {
    val datatype = sf.dataType
    val nullable = sf.nullable
    val cv = new OnHeapColumnVector(numrows, datatype)
    var mismatch = false;
    
    /*
     * Convert and place column data into Spark columnar storage. Comments provide
     * the single character kdb+ data type being processed
     */
    datatype match {
      /* Scalar Types */
      case StringType => cd match {
        case a: Array[Char] => putChars(cv, numrows, a) // c
        case a: Array[String] => putStrings(cv, numrows, a, nullable) // s
        case a: Array[Object] => a(0) match { // Determine type from first row
          case _: Array[Char] => putArrayChars(numrows, a, cv, nullable) // C
          case _: UUID => putUUIDs(numrows, a, cv, nullable) // g
        }
        case _ => mismatch = true
      }

      case BooleanType => cd match {
        case a: Array[Boolean] => putBooleans(cv, numrows, a) // b
        case _ => mismatch = true
      }

      case ByteType => cd match {
        case a: Array[Byte] => cv.putBytes(0, numrows, a, 0) // x
        case _ => mismatch = true
      }

      case ShortType => cd match {
        case a: Array[Short] => putShorts(numrows, a, cv, nullable) // h
        case _ => mismatch = true
      }

      case IntegerType => cd match {
        case a:Array[Int] => putInts(numrows, a, cv, nullable) // i
        case a:Array[c.Minute] => putInts(numrows, a, cv, nullable) // u
        case a:Array[c.Second] => putInts(numrows, a, cv, nullable) // v
        case _ => mismatch = true
      }

      case LongType => cd match {
        case a: Array[Long] => putLongs(numrows, a, cv, nullable) // j
        case _ => mismatch = true
      }

      case FloatType => cd match {
        case a: Array[Float] => putFloats(numrows, a, cv, nullable) // e
        case _ => mismatch = true
      }

      case DoubleType => cd match {
        case a: Array[Double] => putDoubles(numrows, a, cv, nullable) // f
        case _ => mismatch = true
      }

      case TimestampType => cd match {
        case a:Array[JTimestamp] => putTimestamps(numrows, a, cv, nullable) // p
        case a:Array[java.util.Date] => putTimestamps(numrows, a, cv, nullable) // t
        case a:Array[c.Timespan] => putTimestamps(numrows, a, cv, nullable) // n
        case _ => mismatch = true
      }          

      case DateType => cd match {
        case a:Array[JDate] => putDates(numrows, a, cv, nullable)  // d
        case a:Array[c.Month] => putMonths(numrows, a, cv, nullable) // m
        case _ => mismatch = true
      }

      /* Array Types */
      case Type.BooleanArrayType => putBooleanArray(numrows, cd.asInstanceOf[Array[Object]], cv, nullable) // B
      case Type.ByteArrayType => putArray(numrows, cd.asInstanceOf[Array[Object]], cv, nullable) // X
      case Type.ShortArrayType => putArray(numrows, cd.asInstanceOf[Array[Object]], cv, nullable) // H
      case Type.IntegerArrayType => putArray(numrows, cd.asInstanceOf[Array[Object]], cv, nullable) // I
      case Type.LongArrayType => putArray(numrows, cd.asInstanceOf[Array[Object]], cv, nullable) // J
      case Type.FloatArrayType => putArray(numrows, cd.asInstanceOf[Array[Object]], cv, nullable) // E
      case Type.DoubleArrayType => putArray(numrows, cd.asInstanceOf[Array[Object]], cv, nullable) // F
      case Type.TimestampArrayType => putTimestampArray(numrows, cd.asInstanceOf[Array[Object]], cv, nullable) // P
      case Type.DateArrayType => putDateArray(numrows, cd.asInstanceOf[Array[Object]], cv, nullable) // D
      case _ => throw new Exception("Unsupported data type: " + datatype)
    }

    if (mismatch)
      throw new Exception(s"Expecting: $datatype for Spark column: ${sf.name}")

    cv
  }
  
  /*
   * The functions below place the kdb+ column data <cd> into the Spark column storage <cv>
   */
  
  private def putChars(cv: OnHeapColumnVector, numrows: Int, cd: Array[Char]): Unit = {
    val bytes = cd.mkString.getBytes(StandardCharsets.UTF_8)        
    for (rowind <- 0 until numrows) {
      cv.putByteArray(rowind, bytes, rowind, 1)
    }
  }
   
  private def putStrings(cv: OnHeapColumnVector, numrows: Int, cd: Array[String], nullable: Boolean): Unit = {
    for (rowind <- 0 until numrows) {
      val b = cd(rowind).getBytes(StandardCharsets.UTF_8)
      cv.putByteArray(rowind, b, 0, b.length)     
      if (nullable & b.isEmpty)
        cv.putNull(rowind)
    }
  }
 
  private def putShorts(numrows: Int, cd: Array[Short], cv: OnHeapColumnVector, nullable: Boolean): Unit = {
    cv.putShorts(0, numrows, cd, 0)
    if (nullable) {
      for (rowind <- 0 until numrows)
        if (cd(rowind) == Type.ShortNull)
          cv.putNull(rowind)      
    }
  }   
  
  private def putInts(numrows: Int, cd: Array[Int], cv: OnHeapColumnVector, nullable: Boolean): Unit = {
    cv.putInts(0, numrows, cd, 0)
    if (nullable) {
      for (rowind <- 0 until numrows)
        if (cd(rowind) == Type.IntNull)
          cv.putNull(rowind)
    }
  }
  
  private def putInts(numrows: Int, cd: Array[c.Minute], cv: OnHeapColumnVector, nullable: Boolean): Unit = {
    for (rowind <- 0 until numrows) {
      val minute = cd(rowind).i
      cv.putInt(rowind, minute)
      if (nullable && minute == Type.IntNull)
        cv.putNull(rowind)
    }
  }
  
  private def putInts(numrows: Int, cd: Array[c.Second], cv: OnHeapColumnVector, nullable: Boolean): Unit = {
    for (rowind <- 0 until numrows) {
      val second = cd(rowind).i
      cv.putInt(rowind, second)
      if (nullable && second == Type.IntNull)
        cv.putNull(rowind)
    }
  }
  
  private def putLongs(numrows: Int, cd: Array[Long], cv: OnHeapColumnVector, nullable: Boolean): Unit = {
    cv.putLongs(0, numrows, cd, 0)
    if (nullable) {
      for (rowind <- 0 until numrows)
        if (cd(rowind) == Type.LongNull)
          cv.putNull(rowind)
    }
  }          
          
  private def putFloats(numrows: Int, cd: Array[Float], cv: OnHeapColumnVector, nullable: Boolean): Unit = {
    cv.putFloats(0, numrows, cd, 0)
    if (nullable) {
      for (rowind <- 0 until numrows)
        if (cd(rowind).isNaN)
          cv.putNull(rowind)
    }
  }
  
  private def putDoubles(numrows: Int, cd: Array[Double], cv: OnHeapColumnVector, nullable: Boolean): Unit = {
    cv.putDoubles(0, numrows, cd, 0)
    if (nullable) {
      for (rowind <- 0 until numrows)
        if (cd(rowind).isNaN)
          cv.putNull(rowind)
    }
  }
  
  private def putTimestamps(numrows: Int, cd: Array[JTimestamp], cv: OnHeapColumnVector, nullable: Boolean): Unit = {
    for (rowind <- 0 until numrows) {
      val time = cd(rowind).getTime
      cv.putLong(rowind, 1000 * time)
      if (nullable && time == Type.LongNull)
        cv.putNull(rowind)
    }
  }
   
  private def putTimestamps(numrows: Int, cd: Array[java.util.Date], cv: OnHeapColumnVector, nullable: Boolean): Unit = {
    for (rowind <- 0 until numrows) {
      val time = cd(rowind).getTime
      cv.putLong(rowind, 1000 * time)
      if (nullable && time == Type.LongNull)
        cv.putNull(rowind)
    }
  }
  
  private def putTimestamps(numrows: Int, cd: Array[c.Timespan], cv: OnHeapColumnVector, nullable: Boolean): Unit = {
    for (rowind <- 0 until numrows) {
      val span = cd(rowind).j / 1000 // Spark only supports up to microseconds
      cv.putLong(rowind, span)
      if (nullable && cd(rowind).j == Type.LongNull)
        cv.putNull(rowind)
    }
  }
  
  private def putBooleans(cv: OnHeapColumnVector, numrows: Int, cd: Array[Boolean]): Unit = {
    for (rowind <- 0 until numrows) {
      cv.putBoolean(rowind, cd(rowind))
    }
  }
  
  private def putArrayChars(numrows: Int, cd: Array[Object], cv: OnHeapColumnVector, nullable: Boolean): Unit = {
   for (rowind <- 0 until numrows) {
      val bytes = cd(rowind).asInstanceOf[Array[Char]].mkString.getBytes(StandardCharsets.UTF_8)
      cv.putByteArray(rowind, bytes, 0, bytes.length)

      if (nullable && bytes.isEmpty)
        cv.putNull(rowind)
    }
  }

  private def putUUIDs(numrows: Int, cd: Array[Object], cv: OnHeapColumnVector, nullable: Boolean): Unit = {
   for (rowind <- 0 until numrows) {
      val uuid = cd(rowind).asInstanceOf[UUID]
      val bytes = uuid.toString.getBytes()
      cv.putByteArray(rowind, bytes, 0, bytes.length)

      if (nullable && 0 == uuid.compareTo(Type.UUIDNull))
        cv.putNull(rowind)
    }
  }
  
  private def putDates(numrows: Int, cd: Array[JDate], cv: OnHeapColumnVector, nullable: Boolean): Unit = {
    val oneday = 24 * 60 * 60 * 1000 // Milliseconds in a day    
    for (rowind <- 0 until numrows) {
      cv.putInt(rowind, (cd(rowind).getTime / oneday).asInstanceOf[Int])
      if (nullable && cd(rowind).getTime == Type.LongNull)
        cv.putNull(rowind)
    }  
  }
  
  private def putMonths(numrows: Int, cd: Array[c.Month], cv: OnHeapColumnVector, nullable: Boolean): Unit = {
    val date = java.util.Calendar.getInstance
    val oneday = 24 * 60 * 60 * 1000 // Milliseconds in a day   
    for (rowind <- 0 until numrows) {
      if (nullable && cd(rowind).i == Type.IntNull)
        cv.putNull(rowind)
      else {
        date.set(2000 + cd(rowind).i / 12, cd(rowind).i % 12, 1, 0, 0, 0)
        cv.putInt(rowind, (date.getTime.getTime / oneday).asInstanceOf[Int])              
      }
    }    
  }
  
  private def putArray(numrows: Int, cd: Array[Object], cv: OnHeapColumnVector, nullable: Boolean): Unit = {
    var numelem = 0
    var len = 0
    
    for (rowind <- 0 until numrows) {
      cd(rowind) match {
        case a:Array[Byte] => len = a.length; cv.arrayData.appendBytes(len, a, 0)
        case a:Array[Short] => len = a.length; cv.arrayData.appendShorts(len, a, 0)
        case a:Array[Int] => len = a.length; cv.arrayData.appendInts(len, a, 0)
        case a:Array[Long] => len = a.length; cv.arrayData.appendLongs(len, a, 0)
        case a:Array[Float] => len = a.length; cv.arrayData.appendFloats(len, a, 0)
        case a:Array[Double] => len = a.length; cv.arrayData.appendDoubles(len, a, 0)
      }

      cv.putArray(rowind, numelem, len)
      numelem += len
            
      if (nullable && len == 0)
        cv.putNull(rowind)
    }
  }


  private def putBooleanArray(numrows: Int, cd: Array[Object], cv: OnHeapColumnVector, nullable: Boolean): Unit = {
    var numelem = 0

    for (rowind <- 0 until numrows) {
      val b = cd(rowind).asInstanceOf[Array[Boolean]]
      val len = b.length

      val ad = cv.arrayData()
      for (i <- 0 until len)
        ad.appendBoolean(b(i))

      cv.putArray(rowind, numelem, len)
      numelem += len

      if (nullable && len == 0)
        cv.putNull(rowind)
    }
  }


  private def putTimestampArray(numrows: Int, cd: Array[Object], cv: OnHeapColumnVector, nullable: Boolean): Unit = {
    var numelem = 0
    
    for (rowind <- 0 until numrows) {
      val ts = cd(rowind).asInstanceOf[Array[JTimestamp]]
      val len = ts.length
      
      val ad = cv.arrayData()      
      for (i <- 0 until len) 
        ad.appendLong(1000 * ts(i).getTime)  

      cv.putArray(rowind, numelem, len) 
      numelem += len
      
      if (nullable && len == 0)
        cv.putNull(rowind)
    }
  }
    
  private def putDateArray(numrows: Int, cd: Array[Object], cv: OnHeapColumnVector, nullable: Boolean): Unit = {
    val oneday = 24 * 60 * 60 * 1000 // Milliseconds in a day  
    
    var numelem = 0
    
    for (rowind <- 0 until numrows) {
      val d = cd(rowind).asInstanceOf[Array[JDate]]
      val len = d.length
      
      val ad = cv.arrayData()      
      for (i <- 0 until len) 
        ad.appendInt((d(i).getTime / oneday).asInstanceOf[Int])

      cv.putArray(rowind, numelem, len) 
      numelem += len
      
      if (nullable && len == 0)
        cv.putNull(rowind)
    }
  }

  override def close(): Unit = {}
}
