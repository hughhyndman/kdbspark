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

package com.kx.spark

import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row


/*
 * Experimental
 * 
 * Starting to look at providing a kdb+ stream writer (sink). The following is a general
 * framework. Doesn't give expected results at the moment as the rows are out of order 
 * between 5 second intervals unless I start with a single executor.
 * 
 * In bash:
 * $ nc -lk 9999
 * 
 * In q -p 5000
 * q)foo:{0N!x;}
 * 
 * In Scala
 * scala> import com.kx.spark.KdbSink
 * scala> import org.apache.spark.sql.streaming.Trigger 
 * scala> val socketDF = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()
 * scala> socketDF.writeStream.trigger(Trigger.ProcessingTime("5 seconds")).foreach(new KdbSink).start
 * 
 * Then start typing single words in the bash/nc session
 */

class  KdbSink(schema: StructType, options: DataSourceOptions) extends ForeachWriter[Row] {
  var con:c = _
  
  def open(partitionId: Long, version: Long): Boolean = {
    println(s"partitionId: $partitionId; version: $version")
    con = new c("localhost", 5000)
    true
  }

  def process(row: Row): Unit = {
  //TODO: Consider refactoring code with WriteTask 
    con.k("foo", row.toString)
  }

  def close(errorOrNull: Throwable): Unit = {
    con.close
    con = null
  }
 }