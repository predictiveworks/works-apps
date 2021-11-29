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

import org.apache.spark.sql.{Dataset, Row}

class KMeansStage(settings:Map[String,String]) {

  def compute(samples:Dataset[Row], featuresCol:String):Unit = {
    /*
     * The [KMeansRector] computes a sample dataset that contains
     * a features columns that is specified as Array[Numeric].
     *
     * This means that appropriate feature engineering has been
     * applied prior to the use of this method.
     *
     * The vectorCol specifies the internal column that has to be
     * built from the `featuresCol` and that is used for training
     * purposes
     */
    val vectorCol = "_vector"
    val trainer = new KMeansTrainer(settings)
    /*
     * Transform features into a dense vector to make the applicable
     * for Apache Spark's KMeans algorithm
     */
    val vectorset = trainer.vectorize(samples, featuresCol, vectorCol)
    val model = trainer.train(vectorset, vectorCol)
    /*
     * Compute silhouette coefficient as metric for this KMeans parameter
     * setting: to this end, the predictions are computed based on the
     * trained model and the vectorized data set
     */
    val predictionCol = "_cluster"
    model.setPredictionCol(predictionCol)

    val predictions = model.transform(vectorset)
    /*
     * The KMeans evaluator computes the silhouette coefficient of the computed
     * predictions as a means to evaluate the quality of the chosen parameters
     */
    val metrics = Evaluator.evaluate(predictions, vectorCol, predictionCol)

  }
}
