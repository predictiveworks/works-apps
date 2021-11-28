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

import de.kp.works.apps.anon.source.{IgniteApi, PostgresApi}
import io.cdap.cdap.api.spark.{AbstractSpark, SparkExecutionContext, SparkMain, SparkSpecification}
import org.apache.spark.Session
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._

class AnonReactor extends AbstractSpark with SparkMain with AnonNames {

  override def run(implicit sec: SparkExecutionContext): Unit = {

    val spec = getContext.getSpecification
    val args = getArgs(spec, sec)
    /*
     * Initialize Apache Spark session; subsequent
     * components of the program refer to this
     * session
     */
    Session.setProperties(args).setSession()
    /*
     * Stage #1: Retrieve data from configured data
     * source; the current implementation supports
     * Apache Ignite (ignite) and Postgres (postgres)
     */
    val dsName = args(DS_NAME)
    val input:DataFrame = dsName match {
      case "ignite" =>
        val ignite = IgniteApi.getInstance(args)
        ignite.read()

      case "postgres" =>
        val postgres = PostgresApi.getInstance(args)
        postgres.read()

      case _ =>
        val now = new java.util.Date()
        throw new IllegalArgumentException(
          s"[ERROR] ${now.toString} - The configured datasource '$dsName' is not supported.")
    }

  }
  /**
   * A helper method to merge configuration and
   * runtime parameters into a single data structure
   */
  private def getArgs(spec: SparkSpecification, sec: SparkExecutionContext):Map[String,String] = {
    /*
     * Retrieve the application configuration (provided at
     * creation time) from the [SparkSpecification].
     *
     * The respective properties are assigned in [AnonSpark]
     */
    val configArgs = spec.getProperties.asScala.toMap
    /*
     * Retrieve runtime arguments from execution context
     */
    val runtimeArgs = sec.getRuntimeArguments.asScala.toMap

    val args = configArgs ++ runtimeArgs
    args

  }

}
