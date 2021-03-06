package org.apache.spark

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

import org.apache.spark.sql.SparkSession

object Session {

  private var properties:Option[Map[String,String]] = None
  private var session: Option[SparkSession] = None

  private def initialize(): Unit = {
    /*
     * Add project specific configurations from
     * properties to Spark session:
     *
     * See Analytics-Zoo or RasterFrames
     */
    val sc = new SparkContext
    val builder = SparkSession.builder()

    val spark = builder.sparkContext(sc).getOrCreate()
    session = Some(spark)

  }

  def getProperties: Map[String, String] = {
    if (properties.isDefined) properties.get
    else
      Map.empty[String,String]
  }

  def setProperties(args:Map[String,String]): Session.type = {
    properties = Some(args)
    this
  }

  def setSession(): Session.type = {
    if (session.isDefined) initialize()
    this
  }

  def getSession: SparkSession = {
    if (session.isDefined) initialize()
    session.get

  }

}
