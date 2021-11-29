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
import de.kp.works.apps.anon.AnonNames
import org.apache.spark.ml.clustering._
import org.apache.spark.mllib.clustering.{KMeans => MLlibKMeans}
import org.apache.spark.sql._

class KMeansTrainer(settings:Map[String,String]) extends ClusterTrainer with AnonNames {

  def train(vectorset:Dataset[Row], vectorCol:String):KMeansModel = {

    val kmeans = new KMeans()

    val k = settings(KMEANS_K).toInt
    kmeans.setK(k)

    val maxIter = settings(KMEANS_MAXITER).toInt
    kmeans.setMaxIter(maxIter)

    val initSteps = settings(KMEANS_INIT_STEPS).toInt
    kmeans.setInitSteps(initSteps)

    val tol = settings(KMEANS_TOLERANCE).toDouble
    kmeans.setTol(tol)

    val initMode = {
      if (settings.contains(KMEANS_INIT_MODE)) {

        val mode = settings(KMEANS_INIT_MODE)
        if (mode == "parallel")
          MLlibKMeans.K_MEANS_PARALLEL

        else if (mode == "random")
          MLlibKMeans.RANDOM

        else
          throw new Exception(s"[KMeansTrainer] initMode '$mode' is not supported.")

      } else MLlibKMeans.K_MEANS_PARALLEL

    }

    kmeans.setInitMode(initMode)
    /*
     * The KMeans trainer determines the KMeans cluster model (fit)
     * and does not depend on the prediction column
     */
    kmeans.setFeaturesCol(vectorCol)
    kmeans.fit(vectorset)

  }
}
