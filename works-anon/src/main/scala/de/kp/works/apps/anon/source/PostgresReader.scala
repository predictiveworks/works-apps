package de.kp.works.apps.anon.source
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
import org.apache.spark.Session
import org.apache.spark.sql.DataFrame

object PostgresApi {

  private var instance:Option[PostgresReader] = None

  def getInstance(settings:Map[String,String]):PostgresReader = {

    if (instance.isEmpty) instance =
      Some(new PostgresReader(settings))

    instance.get

  }
}

class PostgresReader(settings:Map[String,String]) extends SqlReader with AnonNames {

  private val session = Session.getSession
  private val url = settings(DS_URL)

  private val props = new java.util.Properties()
  props.setProperty("driver", "org.postgresql.Driver")

  if (settings.contains(DS_USER))
    props.setProperty("user", settings(DS_USER))

  if (settings.contains(DS_PASS))
    props.setProperty("password", settings(DS_PASS))

  /*
   * The SQL statement to retrieve data records
   * for anomaly detection is provided as runtime
   * argument
   */
  private val readSql = settings.getOrElse(DS_SQL, "")

  override def read(): DataFrame = {
     session.read.jdbc(url, readSql, props)
  }

}
