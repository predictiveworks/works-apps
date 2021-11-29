package de.kp.works.apps.anon.model.ml.cluster
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

import com.google.gson.Gson
import org.apache.spark.sql._

import scala.collection.mutable

object Evaluator {
  /*
   * Reference to Apache Spark regression evaluator
   * as this object is an access wrapper
   */
  private val evaluator = new ClusteringEvaluator()
  /*
   * This evaluator calculates the silhouette coefficient of
   * the computed predictions as a means to evaluate the quality
   * of the chosen parameters.
   *
   * It distinguished two computation methods:
   *
   * - cosine
   * - squaredEuclidean
   *
   */
  def evaluate(predictions: Dataset[Row], vectorCol: String, predictionCol: String): Map[String,Double] = {

    val metrics = mutable.HashMap.empty[String, Double]

    evaluator.setPredictionCol(predictionCol)
    evaluator.setVectorCol(vectorCol)

    evaluator.setMetricName("silhouette")

    val measures = List("cosine", "squaredEuclidean")
    measures.foreach(measure => {

      evaluator.setDistanceMeasure(measure)
      val value = evaluator.evaluate(predictions)

      val metricName = if (measure == "squaredEuclidean")
        s"silhouette_euclidean"

      else s"silhouette_$measure"

      metrics += metricName -> value

    })

    /* Add unused parameters to be schema compliant */
    metrics += "perplexity" -> 0D
    metrics += "likelihood" -> 0D

    metrics.toMap

  }
}
