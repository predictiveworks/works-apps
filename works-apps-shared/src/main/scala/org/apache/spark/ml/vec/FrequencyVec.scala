package org.apache.spark.ml.vec

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
import org.apache.spark.SparkException
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.HasOutputCol
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.mutable

trait FrequencyVecParams extends Params with HasOutputCol {

  final val featureCols = new Param[Array[String]](this, "featureCols",
  "the list of columns that contain categorical values.", (_:Array[String]) => true)

  final val opMode = new Param[String](this, "opMode",
    "The mode of operation of this vectorizer. Values are 'count', 'freq'." +
      " Default is 'freq'.", (_:String) => true)

}

/**
 * [FrequencyVec] transforms a certain table column (inputCol)
 * that contains categorical text values into numeric values.
 *
 * Each categorical value is assigned its occurrence frequency,
 * i.e. the ratio between the total number of rows and each value
 * count.
 *
 * [FrequencyVec] is implemented as [Estimator] to facilitate model
 * persistence. The current version does not support IO operations.
 */
class FrequencyVec(override val uid: String)
  extends Estimator[FrequencyVecModel] with FrequencyVecParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("FrequencyVec"))

  def setFeatureCols(value: Array[String]): this.type = set(featureCols, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setOpMode(value: String): this.type = set(opMode, value)

  override def fit(dataset: Dataset[_]): FrequencyVecModel = {
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
     * Build the frequency vectorization model
     * that contains the value lookup for each
     * feature column.
     */
    val data = mutable.ArrayBuffer.empty[Map[String, Double]]
    $(featureCols).foreach(featureCol => {

      val values = dataset.select(featureCol).rdd.map(_.getAs[String](0))
      val counts = values
        .map(w => (w,1L))
        .reduceByKey{case(x, y) => x + y}
        .collect()

      val total = counts.map(_._2).sum
      val frequencies = counts.map{case(value, count) =>

        val freq = if ($(opMode) == "count") count.toDouble
        else {
          if (total == 0) 0D else count.toDouble / total
        }

        (value, freq)
      }

      data += frequencies.toMap

    })

    copyValues(new FrequencyVecModel(uid, data).setParent(this))
  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): FrequencyVec = defaultCopy(extra)

}

object FrequencyVec extends DefaultParamsReadable[FrequencyVec] {
  override def load(path: String): FrequencyVec = super.load(path)
}

class FrequencyVecModel(override val uid: String, val data: Seq[Map[String,Double]])
  extends Model[FrequencyVecModel] with FrequencyVecParams with MLWritable {

  import FrequencyVecModel._

  def this(data: Seq[Map[String,Double]]) =
    this(Identifiable.randomUID("FrequencyVecModel"), data)

  def setFeatureCols(value: Array[String]): this.type = set(featureCols, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: Dataset[_]): DataFrame = {

    def vectorize(data:Seq[Map[String,Double]]) =
      udf{(row:Row) => {
        val values = data.indices.map(index =>
          data(index)(row.getAs[String](index))).toArray

        Vectors.dense(values)
      }}

    val colstruct = struct($(featureCols).map(col): _*)
    dataset.withColumn($(outputCol), vectorize(data)(colstruct))

  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): FrequencyVecModel = {
    val copied = new FrequencyVecModel(uid, data).setParent(parent)
    copyValues(copied, extra)
  }

  override def write: MLWriter = new FrequencyVecModelWriter(this)

}

object FrequencyVecModel extends MLReadable[FrequencyVecModel] {

  class FrequencyVecModelWriter(instance: FrequencyVecModel) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      throw new SparkException("Method 'saveImpl' is not implemented")
    }
  }


  private class FrequencyVecModelReader extends MLReader[FrequencyVecModel] {

    override def load(path: String): FrequencyVecModel = {
      throw new SparkException("Method 'load' is not implemented")
    }

  }

  override def read: MLReader[FrequencyVecModel] = new FrequencyVecModelReader

  override def load(path: String): FrequencyVecModel = super.load(path)

}
