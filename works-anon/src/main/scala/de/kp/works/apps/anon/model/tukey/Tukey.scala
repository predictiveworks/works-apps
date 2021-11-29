package de.kp.works.apps.anon.model.tukey
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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

object Tukey {
  /**
   * A helper method to compute the sorted
   * quantile of an RDD[Double].
   */
  def computeQuantileSorted(data:RDD[Double],quantile:Int=85):Double = {

    val count = data.count
    if (count == 0) return 0D

    if (count == 1) data.first
    else {

      val n = (quantile.toDouble / 100.0) * count
      val k = math.round(n)

      if (k <= 0) data.first()
      else {

        val index = data.zipWithIndex().map(_.swap)
        if (k >= count)
          index.lookup(count - 1).head

        else
          index.lookup(k - 1).head

      }
    }
  }

  def computeDistance(dataset:Dataset[_],colname:String):Dataset[_] = {

    val session = dataset.sparkSession
    /*
     * We introduce a temporary extra column to perform
     * the Tukey operation
     */
    val castedDs = dataset.withColumn("_tukey", col(colname).cast(DoubleType))

    val values = castedDs.select("_tukey").rdd.map(row => {
      if (row.get(0) == null) 0D else row.getDouble(0)
    })
    /*
     * Compute IQR based lower & upper frequency bound
     */
    val sorted = values.sortBy(x => x)

    val Q25 = computeQuantileSorted(sorted,25)
    val Q75 = computeQuantileSorted(sorted,75)

    val IQR = Q75 - Q25

    val lower = Q25 - 1.5 * IQR
    val upper = Q75 + 1.5 * IQR

    val ival = session.sparkContext.broadcast((lower,upper))
    /*
     * A UDF that is used to classify the values of a data series;
     * anomalies are described by their distance to the Tukey band
     */
    val classify_udf = udf{v:Double => {

      val l = ival.value._1
      val u = ival.value._2

      if (v < l) {
        /*
         * The current value of the data series is below the lower Tukey
         * limit; this is an outlier and will be characterized by its
         * distance to the lower limit
         */
        val d = v - l
        d

      } else if (v > u) {
        /*
         * The current value of the data series is above the upper Tukey
         * limit; this is an outlier and will be characterized by its
         * distance to the upper limit
         */
        val d = v-u
        d

      } else {
        val d = 0D
        d
      }

    }}
    /*
     * This method is designed to build Tukey based distances
     * that can be directly compared with the original values;
     *
     * therefore no additional manipulation such as normalization
     * is introduced
     */
    castedDs.withColumn("tukey_dist",
      classify_udf(col("_tukey"))).drop("_tukey")

  }

}