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

// SPARK-24073 renames DataReaderFactory -> InputPartition and DataReader -> InputPartitionReader.
// Some classes still reflects the old name and causes confusion.



import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.vectorized.ColumnVector
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.sql.sources.v2.reader.partitioning
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources._
import org.apache.spark.sql.sources.v2.writer._
import org.apache.spark.sql.catalyst.InternalRow

import org.apache.log4j.Logger

import java.util.{ArrayList, List => JList, Optional}
import java.nio.charset.StandardCharsets
import java.sql.{Timestamp => JTimestamp, Date => JDate}

import scala.collection.mutable.Map._
import scala.collection.mutable.{Map => MMap}
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions.mapAsScalaMap
import java.util

/* The name of this class is the argument to the format function in Spark */
class kdb extends DataSourceV2
    with ReadSupport
    with WriteSupport
//    with ReadSupportWithSchema
    with DataSourceRegister {
  /* create a reader instance with/without a user-provided schema */
  override def createReader(schema: StructType, options: DataSourceOptions) = new KdbDataSourceReader(schema, options)
  def createReader(options: DataSourceOptions) = new KdbDataSourceReader(null, options)
  
  /* create a writer instance */
  override def createWriter(jobId: String, schema: StructType, mode: SaveMode, options: DataSourceOptions): Optional[DataSourceWriter] = {
    Optional.of(new KdbDataSourceWriter(jobId, schema, mode, options))
  }
  
  /* Short alias for data source name */
  override def shortName(): String = "kdb" //TODO: Doesn't seem to work  
}

class KdbDataSourceReader(var schema: StructType, options: DataSourceOptions) 
    extends DataSourceReader 
    with SupportsScanColumnarBatch 
    with SupportsPushDownFilters
    with SupportsPushDownRequiredColumns {
  val log: Logger = Logger.getLogger("kdb")
  Util.setLevel(options, log)

  /* Dump out all option settings if in debug mode */
  if (log.isDebugEnabled) {
    log.debug("KdbDataSourceReader()")
    for ((k,v) <- options.asMap) 
       log.debug(s"  $k: $v")
  }
    
  val qExprProvided: Boolean = options.get(Opt.QEXPR).orElse("").length > 0
  
  var filters = new Array[Filter](0)   
  var kdbFilters = new Array[Object](0)  
  var requiredSchema: StructType = _

  /* If no schema is provided, get it from the kdb+ function */
  if (schema == null) { 
    if (qExprProvided)
      throw new Exception("Schema must always be provided when a q expression is used")
    
     schema = getQuerySchema // request kdb+ for it   
  }

  override def readSchema(): StructType = {
    if (requiredSchema != null) requiredSchema else schema
  }
  
  override def pruneColumns(requiredSchema: StructType): Unit = {
    if (log.isDebugEnabled) {
      log.debug("pruneColums()")
      requiredSchema.foreach(s => log.debug("  " + s.toString))
    }
    
    this.requiredSchema = requiredSchema
  }

  override def pushedFilters: Array[Filter] = {
    log.debug("pushedFilters()")
    filters
  }
  
  /*
   * Pushes down filters, and returns filters that need to be evaluated after scanning.
   */
  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    if (log.isDebugEnabled) {
      log.debug("KdbDataSourceReader.pushFilters()")
      filters.foreach(f => log.debug("  " + f.toString))
    }

    kdbFilters = new Array[Object](0) // Array of pushdown filters to be serialized to kdb+
    var supported = Array.empty[Filter] // Filters which kdb+ can support    
    
    /* If the kdb+ function doesn't support push-down filters, get Spark to do it */
    if (!canPushFilters) {
      this.filters = supported
      return filters
    }
         
    /*
     * Loop through the filters determining which ones can be supported by kdb+, from which
     * convert to a form that can be serialized and sent to kdb+. 
     */
    var unsupported = Array.empty[Filter] // Filters that kdb+ cannot support
    filters.foreach {f =>
      val o = convFilter(f)
      if (o != null) {
        kdbFilters +:= o 
        supported +:= f
      }
      else
        unsupported +:= f
    }
    
    this.filters = supported
    unsupported
  }

  /*
   * Return whether the kdb+ host supports push-down filters
   */
  private def canPushFilters: Boolean = {
    options.getBoolean(Opt.PUSHFILTERS, Opt.PUSHFILTERSDEF) &&
      !qExprProvided &&
      !options.get(Opt.TABLE).isPresent
  }
     
  /*
   * Create as many read-task instances as specified by the <numPartitions> option
   */
 //! Supports ScanColumnar
  override def planBatchInputPartitions: JList[InputPartition[ColumnarBatch]] = {
      log.debug("KdbDataSourceReader.createBatchDataReaderFactories()")
      val numparts = options.getInt(Opt.NUMPARTITIONS, Opt.NUMPARTITIONSDEF) // Number of partitions

      var rts = new ArrayList[InputPartition[ColumnarBatch]]      
      for (pid <- 0 until numparts) {
        var optionmap = options.asMap // Make copy of mutable map from options
        optionmap.put(Opt.PARTITIONID, pid.toString)
        rts.add(new ReadTask(optionmap, kdbFilters, readSchema))       
      }

      rts
  }
  
  /*
   * Interrogates kdb+ for the schema (meta data) of the query
   */
  def getQuerySchema: StructType = {
    val optionmap = options.asMap // Get copy of mutable map out of options
    optionmap.put(Opt.PARTITIONID, Opt.SCHEMAREQ) // Insert special partition ID indicating schema query

    val obj = KdbCall.schema(optionmap) // Call kdb+ to get schema
        
    /* The schema information is in the form of the result of kdb's 0!meta[table] */
    val flip = obj.asInstanceOf[c.Flip]
    val metaNames = flip.x
    val cols = flip.y(metaNames.indexOf("c")).asInstanceOf[Array[String]]
    val types = flip.y(metaNames.indexOf("t")).asInstanceOf[Array[Char]]
    
    /* Receiving the nullable indicator (n) from kdb+ is optional */
    val ind = metaNames.indexOf("n")
    val nullables = if (ind >= 0) 
      flip.y(ind).asInstanceOf[Array[Boolean]]
    else
      new Array[Boolean](cols.length) // Default is false
    
    /* Create schema from meta received from kdb+ */
    val fields = new Array[StructField](cols.length)
    for (i <- cols.indices)
      fields(i) = StructField(cols(i), Type.mapDataType(types(i)), nullables(i))
    new StructType(fields)    
  }  
  
  /*
   * Convert pushdown filter instances into objects that can be serialized to kdb+
   */
  def convFilter(f: Filter): Object = f match {
    case EqualTo(attribute, value) => Array("eq", attribute, value)
    case EqualNullSafe(attribute, value) => Array("eq", attribute, value)
    case GreaterThan(attribute, value) => Array("gt", attribute, value)
    case GreaterThanOrEqual(attribute, value) => Array("ge", attribute, value)
    case LessThan(attribute, value) => Array("lt", attribute, value)
    case LessThanOrEqual(attribute, value) => Array("le", attribute, value)
    case IsNull(attribute) => Array("isnull", attribute)
    case IsNotNull(attribute) => Array("isnotnull", attribute)
    case In(attribute, values) => Array("in", attribute, values)
    case Not(child) => convNotFilter(child)
    case Or(left, right) => convConjFilter("or", left, right)
    case And(left, right) => convConjFilter("and", left, right) 
    case StringStartsWith(attribute, value) => Array("ssw", attribute, value)
    case StringEndsWith(attribute, value) => Array("sew", attribute, value)
    case StringContains(attribute, value) => Array("sc", attribute, value)
    case _ => null
  }
  
  def convNotFilter(f: Filter): Object = {
    val o = convFilter(f)
    if (o != null) Array("not", o) else null
  }
  
  /* Convert conjunction filter (ie, and/or) */
  def convConjFilter(op: String, l: Filter, r: Filter): Object = {
    val lo = convFilter(l)
    val ro = convFilter(r)
    if (lo != null && ro != null) Array(op, lo, ro) else null
  }
}

class KdbDataSourceWriter(jobid: String, schema: StructType, mode: SaveMode, options: DataSourceOptions) extends DataSourceWriter {
  val log: Logger = Logger.getLogger("kdb")
  Util.setLevel(options, log) 
  
  var optionmap: util.Map[String, String] = options.asMap
  optionmap.put(Opt.JOBID, jobid)
  optionmap.put(Opt.MODE, mode.toString)
  
  if (log.isDebugEnabled) {
    log.debug("KdbDataSourceWriter()")
    log.debug("  options:")
    for ((k,v) <- optionmap) 
       log.debug(s"    $k: $v")
     
    log.debug("  schema:")
    schema.foreach(s => log.debug("    " + s.toString))
  }
  
  override def createWriterFactory(): DataWriterFactory[InternalRow] = {
    new KdbDataWriterFactory(jobid, schema, mode, optionmap)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    log.debug("KdbDataSourceWriter.commit()")
  }
  
  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    log.debug("KdbDataSourceWriter.abort()")
  }  
}

class KdbDataWriterFactory(jobid: String, schema: StructType, mode: SaveMode, optionmap: util.Map[String, String]) extends DataWriterFactory[InternalRow] {
  override def createDataWriter(partitionId: Int, taskId: Long, epochId: Long): DataWriter[InternalRow] = {
    new WriteTask(partitionId, taskId, epochId, jobid, schema, mode, optionmap)
  }
}
