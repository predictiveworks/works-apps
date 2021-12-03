package de.kp.works.spark.ml.vectorize
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

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.mutable

trait IndexVecParams extends Params {

  final val featureCols = new Param[Array[String]](this, "featureCols",
  "the list of columns that contain categorical values.", (_:Array[String]) => true)

  final val vectorCol = new Param[String](this, "vectorCol",
    "the name of the output column.", (_:String) => true)

}

/**
 * [IndexVec] transforms a list of feature columns (featureCols)
 * that contain categorical text values into numeric values.
 *
 * Each categorical value is assigned its occurrence frequency,
 * i.e. the ratio between the total number of rows and each value
 * count.
 *
 * [IndexVec] is implemented as [Estimator] to facilitate model
 * persistence. The current version does not support IO operations.
 */
class IndexVec(override val uid: String) extends Transformer with IndexVecParams {

  def this() = this(Identifiable.randomUID("IndexVec"))

  def setFeatureCols(value: Array[String]): this.type = set(featureCols, value)

  def setVectorCol(value: String): this.type = set(vectorCol, value)

  def transform(dataset:Dataset[_]):DataFrame = {
    /*
      * Validate whether the provided feature columns
      * are [String] columns
      */
    val schema = dataset.schema
    $(featureCols).foreach(featureCol => {
      if (schema(featureCol).dataType != StringType) {
        throw new Exception(s"[${getClass.getName}] supports [StringType] only.")
      }
    })

    transformSchema(dataset.schema, logging = true)
    /*
     * Build the index model that builds the value
     * lookup for each feature column.
     */
    val data = mutable.ArrayBuffer.empty[Map[String, Int]]
    $(featureCols).foreach(featureCol => {

      data += dataset.select(featureCol)
        .distinct
        .collect
        .map(_.getAs[String](0))
        .sorted
        .zipWithIndex
        .toMap

    })

    def vectorize(data:Seq[Map[String,Int]]) =
      udf{(row:Row) => {
        val values = data.indices
          .map(index =>
            data(index)(row.getAs[String](index)).toDouble
          ).toArray

        Vectors.dense(values)
      }}

    val colstruct = struct($(featureCols).map(col): _*)
    dataset.withColumn($(vectorCol), vectorize(data)(colstruct))

  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): IndexVec = defaultCopy(extra)

}
