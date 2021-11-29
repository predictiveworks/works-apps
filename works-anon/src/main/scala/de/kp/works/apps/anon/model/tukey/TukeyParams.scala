package de.kp.works.apps.anon.model.tukey
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

import org.apache.spark.ml.param.{Param, Params}
import org.apache.spark.sql.types._

case class IQREntry(lower:Double,upper:Double,suspect:Double)

case class TukeyVal(valu:Double,spec:String)
case class TukeySum(classifier:Double,suspect:Double)

object TukeyMethod extends Enumeration {
  type TukeyMethod = Value
  val Approximate,Exact = Value
}

trait TukeyParams extends Params {

  final val valueCol = new Param[String](TukeyParams.this, "valueCol",
    "The name of the (numerical) data field", (value:String) => true)

  def getValueCol:String = $(valueCol)

  def getFieldType(field:StructField):String = {

    field.dataType match {
      case BooleanType   => "categorical"
      case ByteType      => "numerical"
      case DateType      => "categorical"
      case DoubleType    => "numerical"
      case FloatType     => "numerical"
      case IntegerType   => "numerical"
      case LongType      => "numerical"
      case ShortType     => "numerical"
      case StringType    => "categorical"
      case TimestampType => "categorical"
      case other => throw new IllegalArgumentException(s"Data type $other is not supported.")
    }

  }
}
