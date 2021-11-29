package de.kp.works.apps.forecast.model.ts
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

import org.apache.spark.ml.param._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object TimeUtils extends Serializable {

  def date_to_timestamp: UserDefinedFunction = udf { date:java.sql.Date => new java.sql.Timestamp(date.getTime)}

  def long_to_timestamp: UserDefinedFunction = udf { time:Long => new java.sql.Timestamp(time)}

  def time_to_timestamp: UserDefinedFunction = udf { time:java.sql.Timestamp => time}

}

trait TimeParams extends Params {

  final val timeCol = new Param[String](TimeParams.this, "timeCol",
    "Name of the timestamp field", (value:String) => true)

  final val valueCol = new Param[String](TimeParams.this, "valueCol",
    "Name of the value field", (value:String) => true)

  /** @group setParam */
  def setTimeCol(value:String): this.type = set(timeCol, value)

  /** @group setParam */
  def setValueCol(value:String): this.type = set(valueCol, value)

  def validateSchema(schema:StructType):Unit = {

    /* TIME FIELD */

    val timeColName = $(timeCol)

    if (!schema.fieldNames.contains(timeColName))
      throw new IllegalArgumentException(s"Time column $timeColName does not exist.")

    val timeColType = schema(timeColName).dataType
    if (!(timeColType == DateType || timeColType == LongType || timeColType == TimestampType)) {
      throw new IllegalArgumentException(s"Data type of time column $timeColName must be DateType, LongType or TimestampType.")
    }

    /* VALUE FIELD */

    val valueColName = $(valueCol)

    if (!schema.fieldNames.contains(valueColName))
      throw new IllegalArgumentException(s"Value column $valueColName does not exist.")

    val valueColType = schema(valueColName).dataType
    valueColType match {

      /* Basic numeric data types */
      case DoubleType  =>
      case FloatType   =>
      case IntegerType =>
      case LongType    =>
      case ShortType   =>

      /* Array of basic numeric data types */
      case ArrayType(DoubleType, _)  =>
      case ArrayType(FloatType, _)   =>
      case ArrayType(IntegerType, _) =>
      case ArrayType(LongType, _)    =>
      case ArrayType(ShortType, _)   =>

      case _ => throw new IllegalArgumentException(s"Data type of value column $valueColName must be a numeric type.")
    }

  }

  def date_to_timestamp: UserDefinedFunction = udf { date:java.sql.Date => new java.sql.Timestamp(date.getTime)}

  def long_to_timestamp: UserDefinedFunction = udf { time:Long => new java.sql.Timestamp(time)}

  def time_to_timestamp: UserDefinedFunction = udf { time:java.sql.Timestamp => time}

  def createTimeset(dataset:Dataset[_]):Dataset[Row] = {
    /*
     * Time transformer operate on a TimestampType column;
     * as a first step, we have to transform the dataset
     */
    val timecol = col($(timeCol))
    val timeset = dataset.schema($(timeCol)).dataType match {

      case DateType => dataset.withColumn($(timeCol),      TimeUtils.date_to_timestamp(timecol))
      case LongType => dataset.withColumn($(timeCol),      TimeUtils.long_to_timestamp(timecol))
      case TimestampType => dataset.withColumn($(timeCol), TimeUtils.time_to_timestamp(timecol))

      case _ => throw new IllegalArgumentException("[TimeParams] Unsupported time data type detected.")

    }

    timeset

  }

}
