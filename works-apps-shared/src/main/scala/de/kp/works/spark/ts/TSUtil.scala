package de.kp.works.spark.ts
/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 * 
 */
import de.kp.works.spark.functions.normalize_timestamp_udf
import de.kp.works.spark.ml.Interpolate
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object TSUtil {

  /**
   * A day in milliseconds
   *
   * IMPORTANT: The time `step` must be cast to [Long];
   * otherwise the timegrid does not cover the right
   * period of time.
   *
   */
  val DAY: Long = (60 * 60 * 24 * 1000).toLong

  val INTERVALS = List(
    "1min",
    "5min",
    "15min",
    "30min",
    "1h",
    "6h",
    "12h",
    "1d")

  private val verbose = true
  /**
   * This method prepares the dataset for subsequent
   * learning tasks by building an equidistant time
   * grid and by filling missing values
   */
  def prepare(datasource:DataFrame, timeCol:String, interval:String):DataFrame = {

    val startts = System.currentTimeMillis   
    /*
     * STEP #1: Restrict the dataset to those columns that
     * specify numeric values; this also covers the timestamp
     */
    val schema = datasource.schema
    val dropCols = schema.fieldNames.filter(fieldName => {
      
      val fieldType = schema(fieldName).dataType
      fieldType match {
        case DoubleType  => false
        case FloatType   => false
        case IntegerType => false
        case LongType    => false
        case _ => true
      }
      
    })
    
    val input = normalize(datasource.drop(dropCols: _*), timeCol, interval)
    /*
     * STEP #2: Build equidistant time grid and normalize
     * the respective time values
     */
    val timegrid = {
      val grid = buildGridDF(input, timeCol)
      normalize(grid, timeCol, interval)
    }

    val ts1 = System.currentTimeMillis
    if (verbose) println(s"Time grid built in ${ts1 - startts} ms")
    /*
     * STEP #3: Interpolate the columns of the dataset
     * one by one
     */
    val valueCols = schema.fieldNames.filter(fieldName => {
      
      val fieldType = schema(fieldName).dataType
      fieldType match {
        case DoubleType  => true
        case FloatType   => true
        case IntegerType => true
        case LongType    => true
        case _ => false
      }
      
    }).filter(fieldName => fieldName != timeCol)
    
    var output = timegrid.join(input, Seq(timeCol), "left_outer").sort(col(timeCol).asc)
    valueCols.foreach(valueCol => {
      /*
       * [Interpolate] combines forward and backward fill 
       * strategies to receive 'realistic' missing values 
       */
      val transformer = new Interpolate().setTimeCol(timeCol).setValueCol(valueCol)
      output = transformer.transform(output)
    })
    
    val ts2 = System.currentTimeMillis
    if (verbose) println(s"Dataset interpolated in ${ts2 - ts1} ms")
    
    output
    
  }  
  /**
   * Interpolate missing timestamps. The following data
   * strategy is used:
   * 
   * Build an equidistant (e.g. daily) dataframe from the loaded
   * target and perform a LEFT OUTER JOIN to join the timegrid
   * and loaded dataframe. 
   * 
   * This approach introduces the missing timestamps and fills
   *  in 'null' values.
   * 
   * This step prepares for linear interpolation to fill the
   * missing values with interpolated ones. 
   * 
   * Performance: The duration of this method is about 1500 ms
   */    
  def interpolate(input:DataFrame, timeCol:String, valueCol:String, interval:String):DataFrame = {
    /*
     * We do normalize the timestamp because there 
     * is a need to merge with other datasets, e.g.
     * with [TimeGrid]
     */
    val start = System.currentTimeMillis

    val normalized = normalize(input.select(timeCol, valueCol), timeCol, interval)
    
    val ts1 = System.currentTimeMillis
    if (verbose) println(s"Dataset retrieved in ${ts1 - start} ms")
    
    val timegrid = normalize(buildGridDF(normalized, timeCol), timeCol, interval)

    val ts2 = System.currentTimeMillis
    if (verbose) println(s"Time grid built in ${ts2 - ts1} ms")
    
    val result = interpolateWithGrid(normalized, timegrid, timeCol, valueCol)

    val ts3 = System.currentTimeMillis
    if (verbose) println(s"Dataset interpolated in ${ts3 - ts2} ms")

    result
    
  }

  def interpolateWithGrid(input:DataFrame, timegrid:DataFrame, timeCol:String, valueCol:String):DataFrame = {
    
    val interpolated = timegrid.join(input, Seq(timeCol), "left_outer").sort(col(timeCol).asc)
    val datasource = interpolated.select(timeCol, valueCol)
    
    /*
     * [Interpolate] combines forward and backward fill 
     * strategies to receive 'realistic' missing values 
     */
    val transformer = new Interpolate().setTimeCol(timeCol).setValueCol(valueCol)
    val transformed = transformer.transform(datasource)
   
    transformed
    
  }

  def normalize(input:DataFrame, timeCol:String, interval:String):DataFrame = {
    
    val normCol = "_normalized"
    val normalized = input.withColumn(normCol, normalize_timestamp_udf(interval)(col(timeCol)))

    normalized.drop(timeCol).withColumnRenamed(normCol, timeCol)

  }
  /**
   * Performance: The duration of this method is about 60 ms
   */
  def buildGridDF(input:DataFrame, timeCol:String, step:Long = DAY):DataFrame = {

    /*
     * A helper method to return a list of equally spaced
     * points between mints and maxts with step size 'step'.
     */
    def timegrid_udf(step:Long) = udf{(mints:Long, maxts:Long) => {

      val steps = ((maxts - mints) / step).toInt + 1
      (0 until steps).map(x => mints + (step * x)).toArray

    }}

    val grid_udf = timegrid_udf(step)
    val timegrid = input
      /*
       * Determine the minimum & maximum value of the time series
       */
      .agg(min(timeCol).cast(LongType).as("mints"), max(timeCol).cast(LongType).as("maxts"))
      /*
       * Compute timegrid and explode values
       */
      .withColumn(timeCol, explode(grid_udf(col("mints"), col("maxts")))).drop("mints").drop("maxts")

    timegrid

  }
  /**
   * This method split a time series in ascending order with
   * respect to the provided split, e.g. 80:20.
   *
   * Performance: The duration of this method is about 6000 ms
   */
  def timeSplit(dataframe:DataFrame, timeCol:String, timeSplit:String): Array[Array[Row]] = {

    val startts = System.currentTimeMillis

    val fractions = timeSplit.split(":").map(token => {
      token.trim.toDouble / 100
    })

    if (fractions.length != 2 || fractions.sum != 1D)
      throw new IllegalArgumentException("[TimeSplit] The time split expects two integer numbers of sum 100.")

    val timeset = dataframe.coalesce(1)

    val maxts = timeset.agg({timeCol -> "max"}).collect.head(0).asInstanceOf[Long]
    val mints = timeset.agg({timeCol -> "min"}).collect.head(0).asInstanceOf[Long]

    /* Compute threshold */
    val threshold = mints + Math.ceil(fractions(0) * (maxts - mints)).toLong

    /* Split timeset */
    val lowerSplit = timeset.filter(col(timeCol) <= threshold).collect
    val upperSplit = timeset.filter(col(timeCol) > threshold).collect

    val result = Array(lowerSplit, upperSplit)

    val endts = System.currentTimeMillis
    if (verbose) println(s"Dataset split in ${endts - startts} ms.")

    result

  }

  /**
   * This method split a time series in ascending order with 
   * respect to the provided split, e.g. 80:20.
   * 
   * Performance: The duration of this method is about 500 ms
   * 
   */
  def timeSplitDF(dataframe:DataFrame, timeCol:String, timeSplit:String): Array[DataFrame] = {
      
    val startts = System.currentTimeMillis
    
    val fractions = timeSplit.split(":").map(token => {
        token.trim.toDouble / 100
    })
    
    if (fractions.length != 2 || fractions.sum != 1D)
      throw new IllegalArgumentException("[TimeSplit] The time split expects two integer numbers of sum 100.")
    
    def threshold_udf(fractions:Array[Double]) = udf((mints:Long, maxts:Long) => {

      val threshold = mints + Math.ceil(fractions(0) * (maxts - mints)).toLong 
      threshold

    })

    dataframe.createOrReplaceTempView("t1")

    val timeframe = dataframe
      .agg(min(timeCol).cast(LongType).as("mints"), max(timeCol).cast(LongType).as("maxts"))
      .withColumn("threshold", threshold_udf(fractions)(col("mints"), col("maxts")))

    timeframe.createOrReplaceTempView("t2")
    
    val session = dataframe.sparkSession
    /*
     * Split with SQL statements
     */
    val dropcols = Array("maxts", "mints", "threshold")
    
    val lowerSql = s"select * from t1, t2 where t1.$timeCol <= t2.threshold"
    val lower = session.sql(lowerSql).drop(dropcols: _*)
    
    val upperSql = s"select * from t1, t2 where t1.$timeCol > t2.threshold"
    val upper = session.sql(upperSql).drop(dropcols: _*)
    
    val result = Array(lower, upper)
    
    val endts = System.currentTimeMillis
    if (verbose) println(s"Dataset split in ${endts - startts} ms.")
    
    result
    
  }

}