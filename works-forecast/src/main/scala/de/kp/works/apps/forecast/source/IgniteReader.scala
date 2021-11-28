package de.kp.works.apps.forecast.source

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

import de.kp.works.apps.forecast.ForecastNames
import org.apache.spark.Session
import org.apache.spark.sql.DataFrame

object IgniteApi {

  private var instance:Option[IgniteReader] = None

  def getInstance(settings:Map[String,String]):IgniteReader = {

    if (instance.isEmpty) instance =
      Some(new IgniteReader(settings))

    instance.get

  }
}

class IgniteReader(settings:Map[String,String]) extends SqlReader with ForecastNames {

  private val session = Session.getSession
  /*
   * The SQL statement to retrieve data records
   * for anomaly detection is provided as runtime
   * argument
   */
  private val readSql = settings.getOrElse(DS_SQL, "")
  override def read(): DataFrame = ???

}
