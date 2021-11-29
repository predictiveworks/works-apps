package de.kp.works.apps.anon
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

trait AnonNames {
  /*
   * Datasource specific parameters
   */
  val DS_NAME = "ds.name"
  val DS_PASS = "ds.password"
  val DS_SQL  = "ds.sql"
  val DS_URL  = "ds.url"
  val DS_USER = "ds.user"
  /*
   * Model specific parameters
   */
  val ANON_MODEL = "anon.model"
  /*
   * KMeans specific parameters: The respective
   * values are provided as realtime arguments.
   *
   * This enable a flexible (re-use) of the
   * application without the need to redeploy
   * it.
   */
  val KMEANS_K          = "anon.kmeans.k"
  val KMEANS_INIT_MODE  = "anon.kmeans.initMode"
  val KMEANS_INIT_STEPS = "anon.kmeans.initSteps"
  val KMEANS_MAXITER    = "anon.kmeans.maxIter"
  val KMEANS_TOLERANCE  = "anon.kmeans.tolerance"

}
