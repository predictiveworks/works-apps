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
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object TSUtil {

  /**
   * IMPORTANT: The time `step` must be cast to [Long];
   * otherwise the time grid does not cover the right
   * period of time.
   */
  val MIN_1: Long   = (60 * 1000).toLong
  val MIN_5: Long   = (5 * 60 * 1000).toLong
  val MIN_15: Long  = (15 * 60 * 1000).toLong
  val MIN_30: Long  = (30 * 60 * 1000).toLong
  val HOUR_1: Long  = (60 * 60 * 1000).toLong
  val HOUR_6: Long  = (6 * 60 * 60 * 1000).toLong
  val HOUR_12: Long = (12 * 60 * 60 * 1000).toLong
  val DAY_1: Long   = (24 * 60 * 60 * 1000).toLong

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
   * This method prepares the dataset for subsequent learning tasks
   * by building an equidistant time grid and by filling (interpolating)
   * missing values.
   *
   * It is also the starting point for later aggregation tasks.
   *
   * __HINT__
   *
   * This method restricts (or reduces) the provided datasources to numeric
   * columns (double, float, integer and long).
   *
   */
  def prepare(input:DataFrame, timeCol:String,
              timeInterval:String, timeStep:Long):DataFrame = {

    val startts = System.currentTimeMillis
    /*
     * STEP #1: Restrict the dataset to those columns that
     * specify numeric values; this also covers the timestamp
     */
    val schema = input.schema
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
    /*
     * STEP #2: Normalize the timestamp to the best matching
     * 1, 5, 15, 30 minute and 1, 6, 12 hour and 1 day.
     */
    val normalized = normalize(
      input.drop(dropCols: _*), timeCol, timeInterval)

    val ts1 = System.currentTimeMillis
    if (verbose) println(s"Dataset normalized in ${ts1 - startts} ms")
    /*
     * STEP #3: Build equidistant time grid and normalize
     * the respective (filled) timestamps again.
     */
    val timegrid = normalize(
      buildTimeGrid(normalized, timeCol, timeStep), timeCol, timeInterval)

    val ts2 = System.currentTimeMillis
    if (verbose) println(s"Time grid built in ${ts1 - ts1} ms")
    /*
     * STEP #4: Join the computed time grid with the provided
     * and normalized datasource and sort by timestamp in ascending
     * order.
     *
     * This operation creates a set of missing values for those
     * timestamps (of the grid) that do not have assigned values.
     */
    var output = timegrid.join(normalized, Seq(timeCol), "left_outer").sort(col(timeCol).asc)
    /*
     * STEP #5: Determine the value columns and interpolate the
     * missing values for each column.
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
    valueCols.foreach(valueCol => {
      /*
       * [Interpolate] combines forward and backward fill
       * strategies to receive 'realistic' missing values
       */
      val transformer = new Interpolate().setTimeCol(timeCol).setValueCol(valueCol)
      output = transformer.transform(output)
    })

    val ts3 = System.currentTimeMillis
    if (verbose) println(s"Dataset interpolated in ${ts3 - ts2} ms")

    output

  }
  /**
   * This method prepares the dataset for subsequent learning tasks
   * by building an equidistant time grid and by filling (interpolating)
   * missing values.
   *
   * Performance: The duration of this method is about 1500 ms
   */
  def prepare(input:DataFrame, timeCol:String, valueCol:String,
              timeInterval:String, timeStep:Long):DataFrame = {

    val startts = System.currentTimeMillis
    /*
     * STEP #1: Normalize the timestamp to the best matching
     * 1, 5, 15, 30 minute and 1, 6, 12 hour and 1 day.
     */
    val normalized = normalize(
      input.select(timeCol, valueCol), timeCol, timeInterval)

    val ts1 = System.currentTimeMillis
    if (verbose) println(s"Dataset normalized in ${ts1 - startts} ms")
    /*
     * STEP #2: Build equidistant time grid and normalize
     * the respective (filled) timestamps again.
     */
    val timegrid = normalize(
      buildTimeGrid(normalized, timeCol, timeStep), timeCol, timeInterval)

    val ts2 = System.currentTimeMillis
    if (verbose) println(s"Time grid built in ${ts2 - ts1} ms")
    /*
     * STEP #3: Join the computed time grid with the provided
     * and normalized datasource and sort by timestamp in ascending
     * order.
     *
     * This operation creates a set of missing values for those
     * timestamps (of the grid) that do not have assigned values.
     */
    var output = timegrid.join(normalized, Seq(timeCol), "left_outer").sort(col(timeCol).asc)
    /*
     * [Interpolate] combines forward and backward fill
     * strategies to receive 'realistic' missing values
     */
    val transformer = new Interpolate().setTimeCol(timeCol).setValueCol(valueCol)
    output = transformer.transform(output)

    val ts3 = System.currentTimeMillis
    if (verbose) println(s"Dataset interpolated in ${ts3 - ts2} ms")

    output

  }
  /**
   * Timeseries refer to fluctuating timestamps; this makes it
   * difficult to build equidistant time grids for forecasting
   * or other deep & machine learning asks.
   *
   * This method normalizes timestamp to the best matching
   * 1, 5, 15, 30 minutes and 1, 6, 12 hours and 1 day.
   */
  def normalize(input:DataFrame, timeCol:String, interval:String):DataFrame = {

    val normCol = "_normalized"
    val normalized = input.withColumn(normCol, normalize_timestamp_udf(interval)(col(timeCol)))

    normalized.drop(timeCol).withColumnRenamed(normCol, timeCol)

  }
  /**
   * This method determines the minimum and maximum timestamp
   * of the provided timeseries and builds an equidistant grid
   * based on the specific step size
   *
   * Performance: The duration of this method is about 60 ms
   */
  def buildTimeGrid(input:DataFrame, timeCol:String, timeStep:Long):DataFrame = {
    /*
     * A helper method to return a list of equally spaced
     * points between mints and maxts with step size 'step'.
     */
    def timegrid_udf(step:Long) = udf{(mints:Long, maxts:Long) => {

      val steps = ((maxts - mints) / step).toInt + 1
      (0 until steps).map(x => mints + (step * x)).toArray

    }}

    val grid_udf = timegrid_udf(timeStep)
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
   * Performance: The duration of this method is about 500 ms
   * 
   */
  def buildTimeSplit(dataframe:DataFrame, timeCol:String, timeSplit:String): Array[DataFrame] = {
      
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