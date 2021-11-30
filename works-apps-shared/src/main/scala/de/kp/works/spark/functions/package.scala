package de.kp.works.spark
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
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import java.util.Calendar

package object functions {

  def date_to_timestamp: UserDefinedFunction =
    udf { date:java.sql.Date => new java.sql.Timestamp(date.getTime)}

  def long_to_timestamp: UserDefinedFunction =
    udf { time:Long => new java.sql.Timestamp(time)}

  def minmax_scaler_udf(minval:Double, maxval:Double): UserDefinedFunction =
    udf((value:Double) => {
      try {
        val band = maxval - minval
        (value - minval) / band
        
      } catch {
        case _:Throwable => Double.NaN
      }
    })

  def normalize_timestamp_udf(interval:String): UserDefinedFunction =
    udf((timestamp:Long) => {

    val c = java.util.Calendar.getInstance()
    c.setTimeInMillis(timestamp)

    interval match {
      case "1min" =>
        val s = c.get(Calendar.SECOND)
        if (s >= 30) c.add(Calendar.MINUTE, 1)

        c.set(Calendar.SECOND, 0)
        c.set(Calendar.MILLISECOND, 0)

      case "5min" =>
        val m = c.get(Calendar.MINUTE)
        if (m > 0) {
          val mod = m % 5
          c.add(Calendar.MINUTE, -mod)
        }

        c.set(Calendar.SECOND, 0)
        c.set(Calendar.MILLISECOND, 0)

      case "15min" =>
        val m = c.get(Calendar.MINUTE)
        if (m > 0) {
          val mod = m % 15
          c.add(Calendar.MINUTE, -mod)
        }

        c.set(Calendar.SECOND, 0)
        c.set(Calendar.MILLISECOND, 0)

      case "30min" =>
        val m = c.get(Calendar.MINUTE)
        if (m > 0) {
          val mod = m % 30
          c.add(Calendar.MINUTE, -mod)
        }

        c.set(Calendar.SECOND, 0)
        c.set(Calendar.MILLISECOND, 0)

      case "1h" =>
        val min = c.get(Calendar.MINUTE)
        if (min >= 30) c.add(Calendar.HOUR, 1)

        c.set(Calendar.MINUTE, 0)
        c.set(Calendar.SECOND, 0)
        c.set(Calendar.MILLISECOND, 0)

      case "6h" =>
        val h = c.get(Calendar.HOUR)
        if (h > 0) {
          val mod = h % 6
          c.add(Calendar.HOUR, -mod)
        }

        c.set(Calendar.MINUTE, 0)
        c.set(Calendar.SECOND, 0)
        c.set(Calendar.MILLISECOND, 0)

      case "12h" =>
        val h = c.get(Calendar.HOUR)
        if (h > 0) {
          val mod = h % 12
          c.add(Calendar.HOUR, -mod)
        }

        c.set(Calendar.MINUTE, 0)
        c.set(Calendar.SECOND, 0)
        c.set(Calendar.MILLISECOND, 0)

      case "1d" =>
        val h = c.get(Calendar.HOUR)
        if (h >= 12) c.add(Calendar.DATE, 1)

        c.set(Calendar.HOUR, 0)
        c.set(Calendar.MINUTE, 0)
        c.set(Calendar.SECOND, 0)
        c.set(Calendar.MILLISECOND, 0)

      case _ =>
        throw new Exception("The provided time interval is not supported for bucketing")

    }

  })

  def timestamp_udf: UserDefinedFunction =
    udf((timestamp:java.sql.Timestamp) => timestamp.getTime)

  def time_to_timestamp: UserDefinedFunction =
    udf { time:java.sql.Timestamp => time}

}