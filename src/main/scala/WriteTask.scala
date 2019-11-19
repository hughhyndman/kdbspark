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


import java.sql.{Timestamp => JTimestamp, Date => JDate}
import java.util.Map

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.sources.v2.writer._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.log4j.Logger

import scala.collection.mutable.WrappedArray

//TODO: Must test with multiple Spark partitions (partitionId?)

class WriteTask(partitionId: Int, taskId: Long, epochId: Long, jobid: String, schema: StructType, mode: SaveMode, optionmap: Map[String,String]) 
    extends DataWriter[InternalRow] {
  @transient lazy val options = new DataSourceOptions(optionmap)
  @transient lazy val log = Logger.getLogger("kdbdw")
  
  var batch: Array[Object] = null
  var batchingSize = 0 // Number of rows per batch
  var batchCount = 1 // Current batch number
  var rowCount = 0 // Total number of rows written 
  var batchind = 0 // Index into batch "rows"
  
  override def write(row: InternalRow): Unit = { 
    /*
     * If this is the first write, initialize the batch given the columns provided
     * in the schema. A batch is a set of column arrays.
     */
    if (batch == null) {
      batchingSize = options.getInt("batchingsize", 10000)
      if (log.isDebugEnabled) {
        log.debug("write(): first call")
        log.debug(s"  batchingsize:  $batchingSize")
        log.debug(s"  partitionId:   $partitionId")
//!     log.debug(s"  attemptNumber: $attemptNumber")        
      }
      
      batch = new Array[Object](schema.length)
      for (colind <- 0 until schema.length) {
        batch(colind) = createColumnArray(schema(colind).dataType, batchingSize)
      }      
    }
    
    /*
     * Loop through the columns in the provided row, placing the data into the batch
     * column arrays
     */
    for (i <- 0 until schema.length) {
      val nn = !row.isNullAt(i); //! row(i) != null; // Not null
      val bi = batch(i);
      val dt = schema(i).dataType;
      val ob = row.get(i, dt);
      val rt = row.get(i, TimestampType);
      val oneday = 24 * 60 * 60 * 1000 
      log.debug(s"i=$i; bi=$bi; dt=$dt; rt=$rt; ob=$ob");      
      
      batch(i) match { 
        case a:Array[Boolean] => a(batchind) = row.getBoolean(i)
        case a:Array[Byte] => a(batchind) = row.getByte(i) //! row.getAs[Byte](i)
        case a:Array[Short] => a(batchind) = if (nn) row.getShort(i) else Kdb.ShortNull
        case a:Array[Int] => a(batchind) = if (nn) row.getInt(i) else Kdb.IntNull
        case a:Array[Long] => a(batchind) = if (nn) row.getLong(i) else Kdb.LongNull
        case a:Array[Float] => a(batchind) = if (nn) row.getFloat(i) else Kdb.FloatNull
        case a:Array[Double] => a(batchind) = if (nn) row.getDouble(i) else Kdb.DoubleNull
        case a:Array[JTimestamp] => a(batchind) = if (nn) new JTimestamp(row.getLong(i) / 1000) else Kdb.TimestampNull
//! Somehow the TZ crept in below
        case a:Array[JDate] => a(batchind) = if (nn) new JDate(row.getLong(i) * oneday) else Kdb.DateNull
        
        case a:Array[Object] => a(batchind) = 
          schema(i).dataType match {
            case StringType => if (nn) row.getString(i).toCharArray.asInstanceOf[Object] else Kdb.StringNull
            case Kdb.LongArrayType => row.getArray(i).asInstanceOf[WrappedArray[Array[Long]]].array
            
 /*
            case Kdb.ByteArrayType => row(i).asInstanceOf[WrappedArray[Array[Byte]]].array
            case Kdb.ShortArrayType => row(i).asInstanceOf[WrappedArray[Array[Short]]].array
            case Kdb.IntegerArrayType => row(i).asInstanceOf[WrappedArray[Array[Int]]].array

            case Kdb.FloatArrayType => row(i).asInstanceOf[WrappedArray[Array[Float]]].array
            case Kdb.DoubleArrayType => row(i).asInstanceOf[WrappedArray[Array[Double]]].array
            case Kdb.TimestampArrayType => row(i).asInstanceOf[WrappedArray[Array[JTimestamp]]].array
            case Kdb.DateArrayType => row(i).asInstanceOf[WrappedArray[Array[JDate]]].array
 */
            case _ => throw new Exception("Unsupported data type: " + schema(i).dataType) 
          }
        case _ => throw new Exception("Unsupported data type: " + batch(i).getClass)
      }
    }
    
    /* If we filled a batch; send it to kdb+ */
    batchind += 1
    if (batchind == batchingSize) {
      writeBatch("write")      
      batchCount += 1
      batchind = 0
    }
  }
    
  override def commit(): WriterCommitMessage = {
    truncateBatch(batchind) // Resize batch to fit remaining rows
    writeBatch("commit")  
    batch = null; // Free memory        
    null
  }
  
  override def abort(): Unit = {
    truncateBatch(0)
    writeBatch("abort")  
    batch = null; // Free memory
  }
  
  /* Send batch to kdb+ specifying write disposition */
  private def writeBatch(disp: String): Unit = {
    optionmap.put("writeaction", disp) 
    optionmap.put("batchcount", batchCount.toString)
    Kdb.write(optionmap, schema, batch)
    rowCount += batchind
    log.debug(s"Batches written: $batchCount; Rows written: $rowCount")    
  }
  
  /* Truncate batch by keeping the first n rows */
  def truncateBatch(n: Int) {
    for (colind <- 0 until batch.length) {
      batch(colind) = batch(colind) match {
        case a:Array[Boolean] => a.take(n)
        case a:Array[Byte] => a.take(n)
        case a:Array[Short] => a.take(n)
        case a:Array[Int] => a.take(n)
        case a:Array[Long] => a.take(n)
        case a:Array[Float] => a.take(n)
        case a:Array[Double] => a.take(n)
        case a:Array[JTimestamp] => a.take(n)
        case a:Array[JDate] => a.take(n)
        case a:Array[Object] => a.take(n) // All kdb+ lists (arrays) handled here
      }
    }
  }
  
  /* Create a kdb+ array of length <bs> given a Spark datatype */
  def createColumnArray(dt: DataType, bs: Int): Array[_] = {    
    dt match {
      case BooleanType => new Array[Boolean](bs)
      case ByteType => new Array[Byte](bs)
      case ShortType => new Array[Short](bs)
      case IntegerType => new Array[Int](bs)
      case LongType => new Array[Long](bs)
      case FloatType => new Array[Float](bs)
      case DoubleType => new Array[Double](bs)
      case StringType => new Array[Object](bs)
      case TimestampType => new Array[JTimestamp](bs)
      case DateType => new Array[JDate](bs)
      case Kdb.ByteArrayType => new Array[Object](bs)
      case Kdb.ShortArrayType => new Array[Object](bs)
      case Kdb.IntegerArrayType => new Array[Object](bs)
      case Kdb.LongArrayType => new Array[Object](bs) 
      case Kdb.FloatArrayType => new Array[Object](bs) 
      case Kdb.DoubleArrayType => new Array[Object](bs) 
      case Kdb.TimestampArrayType => new Array[Object](bs)
      case Kdb.DateArrayType => new Array[Object](bs)
      case _ => throw new Exception("Unsupported data type:" + dt)
    }
  }
}