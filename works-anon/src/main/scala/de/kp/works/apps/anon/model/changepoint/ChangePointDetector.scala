package de.kp.works.apps.anon.model.changepoint
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

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable

trait ChangePointParams extends Params {

  /*** COLUMNS ***/

  final val tsCol = new Param[String](this, "tsCol",
    "Name of the timestamp field", (value:String) => true)

  final val valueCol = new Param[String](this, "valueCol",
    "Name of the value field", (value:String) => true)

  /** @group getParam */
  def getTsCol:String = $(tsCol)

  /** @group getParam */
  def getValueCol:String = $(valueCol)


  /*** PARAMETERS ***/

  final val window = new Param[Int](this, "window",
    "Number of samples which affects change-point score [default: 30]", (value:Int) => true)

  final val threshold = new Param[Double](this, "threshold",
    "Score threshold (inclusive) for determining change-point existence [default: -1, do not output decision]", (value:Double) => true)

  /** @group getParam */
  def getWindow:Int = $(window)

  /** @group getParam */
  def getThreshold:Double = $(threshold)

  setDefault(window -> 30, threshold -> -1)

}
/**
 * [ChangePoint] is a new method for statistically detecting change points in sensor data.
 *
 * Sensors mounted on devices like IoT devices, automated manufacturing like robot arms,
 * process monitoring and control equipment etc., collect and transmit data on a continuous
 * basis which is timestamped.
 *
 * [ChangePoint] collates statistics on such time series data and identifies if a change point
 * has occurred. Core building blocks include computing statistical parameters from the time
 * series, which compares a previous dataset of a certain time range in the past with the current
 * series in a recent time range.
 *
 * Statistical comparison between these two results in detection of any change points.
 *
 */
class ChangePointDetector (override val uid: String) extends Transformer with ChangePointParams {

  def this() = this(Identifiable.randomUID("changePointDetector"))

  /*** COLUMNS ***/

   def setTsCol(value:String): this.type = set(tsCol, value)

  def setValueCol(value:String): this.type = set(valueCol, value)

  /*** PARAMETERS ***/

  def setWindow(value:Int): this.type = set(window, value)

  def setThreshold(value:Double): this.type = set(threshold, value)

  def transform(dataset:Dataset[_]):DataFrame = {

    /* Check parameter values */
    if ($(window) < 2)
      throw new IllegalArgumentException("window must be greater than 1: " + $(window))

    if ($(threshold) <= 0D || $(threshold) >= 1D)
      throw new IllegalArgumentException("Threshold must be in range (0, 1): " + $(threshold))

    /* Transform schema if required */
    transformSchema(dataset.schema, logging = true)
    /*
     * This algorithm is a window-based change point detection algorithm,
     * that leverages the Window feature of Apache Spark SQL:
     *
     * As a first step, we have to transform the values of the provided
     * number of previous rows and subsequent rows into vectors, i.e. we
     * have to embed the timeseries values into a vector space
     *
     */
    val spec = Window.partitionBy().rowsBetween(-$(window), $(window))
    val windowed = dataset.withColumn("vector", collect_list(col($(valueCol)).cast(DoubleType)).over(spec))

    val filter = filter_udf($(window))
    val filtered = windowed.filter(filter(col("vector")))

    val score = score_udf($(window))
    val scored = filtered.withColumn("cp_score", score(col("vector")))

    val detect = detect_udf($(threshold))
    val detected = scored.withColumn("is_cp", detect(col("cp_score")))

    detected.drop("vector")

  }

  private def filter_udf(window:Int) = udf{observation:mutable.WrappedArray[Double] =>
    observation.length == window * 2 + 1
  }

  private def detect_udf(threshold:Double) = udf{score: Double =>
    score > threshold
  }

  private def score_udf(window:Int) = udf{
    (observation: mutable.WrappedArray[Double]) => {

      val prev = observation.slice(0, window)
      val next = observation.slice(window, observation.length - 1)

      // TODO
      0D
    }
  }

  override def transformSchema(schema:StructType):StructType = {
    schema
  }

  override def copy(extra:ParamMap):ChangePointDetector = defaultCopy(extra)

}