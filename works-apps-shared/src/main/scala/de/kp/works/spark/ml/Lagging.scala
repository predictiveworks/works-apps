package de.kp.works.spark.ml

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
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.{Param, ParamMap, ParamValidators}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable

case class Multivariate(label: Seq[Seq[Double]], features: Seq[Seq[Double]])

case class Univariate(label: Vector, features: Vector)

trait TimeLaggingParams extends TimeParams {

  final val featuresCol = new Param[String](TimeLaggingParams.this, "featuresCol",
    "Name of the column that contains the feature values", (_:String) => true)

  final val labelCol = new Param[String](TimeLaggingParams.this, "labelCol",
    "Name of the column that contains the label value(s)", (_:String) => true)

  final val lag = new Param[Int](TimeLaggingParams.this, "lag",
    "The number of past points of time to take into account for vectorization.", (_:Int) => true)

  final val laggingType = new Param[String](this, "laggingType",
    "The selector type of the lagging algorithm. Supported values are: 'features', 'pastFeatures' and 'featuresAndLabels'.",
    ParamValidators.inArray[String](Array("features", "pastFeatures", "featuresAndLabels")))

  /* __MOD__ */

  final val frame = new Param[Int](TimeLaggingParams.this, "frame",
    "The number of future points of time to take into account for vectorization.", (_:Int) => true)

  final val dimensionality = new Param[String](this, "dimensionality",
    "The specification of the dimensionality of the values. Supported values are: 'multivariate', and 'univariate'.",
    ParamValidators.inArray[String](Array("multivariate", "univariate")))

  def setFeaturesCol(value:String): this.type = set(featuresCol, value)

  def setLabelCol(value:String): this.type = set(labelCol, value)

  def setLag(value:Int): this.type = set(lag, value)

  def setLaggingType(value:String): this.type = set(laggingType, value)

  /* __MOD__ */

  def setDimensionality(value:String): this.type = set(dimensionality, value)

  def setFrame(value:Int): this.type = set(frame, value)

  setDefault(
    dimensionality -> "univariate",
    featuresCol    -> "features",
    labelCol       -> "label",
    lag            -> 10,
    laggingType    -> "featuresAndLabels",
    /* Label dimension */
    frame          -> 1)

  override def validateSchema(schema:StructType):Unit = {
    super.validateSchema(schema)

  }

}
/**
 * Lagging: vector of past N values
 *
 * The goal of this transformer is to prepare (cleaned) time series data for (demand) prediction:
 *
 * For each value x(t) of the time series, we build the vector x(t-N), ..., x(t-2), x(t-1), x(t).
 * Weuse the past values x(t-N), ..., x(t-2), x(t-1) as feature vector for the prediction model
 * and the current value x(t) as the target column or label to train the model.
 *
 * REMINDER: Build the vector of past N values after partitioning the dataset into a training set
 * and a test set in order to avoid data leakage from neighboring values.
 */
class Lagging(override val uid: String) extends Transformer with TimeLaggingParams {

  def this() = this(Identifiable.randomUID("lagging"))

  def transform(dataset:Dataset[_]):DataFrame = {

    validateSchema(dataset.schema)
    /*
     * This transformer operates on a TimestampType column;
     * we ensure that the time column is formatted as a time
     * stamp.
     *
     * For further processing, we additionally have to turn
     * the time column into a (reversible) LongType
     */
    val timeset = createTimeset(dataset)
    val m = $(lag)
    val n = $(frame)

    if ($(laggingType) == "featuresAndLabels") {
      /*
       * Specify a window with m + n values as we want to take
       * feature vector (m) and target value (n) into account
       */
      val spec = Window.partitionBy().rowsBetween(-m, n - 1)

      val vectorized =
        timeset.withColumn("_vector", collect_list(col($(valueCol))).over(spec))
      /*
       * Select user defined function to separate features
       * and label that matches the provided `dimensionality`
       * of the dataset values
       */
      val labelFeaturesUDF =
        if ($(dimensionality) == "multivariate")
          multivariateLabelFeatures(m, n)

        else
          univariateLabelFeatures(m, n)
      /*
       * Apply user defined function and add two additional
       * columns, `features` and `label` to the dataset
       */
      val result = vectorized
        /*
         * Build features and lables, and remove all null values;
         * null indicates that the length of the feature vector is
         * smaller than 'lag'
         */
        .withColumn("_result", labelFeaturesUDF(col("_vector"))).filter(col("_result").isNotNull)
        /*
         * Transform the result and thereby split into features
         * and label column
         */
        .withColumn($(featuresCol), col("_result").getItem("features"))
        .withColumn($(labelCol), col("_result").getItem("label"))
        /*
         * Remove internal columns
         */
        .drop("_result").drop("_vector")

      /*
       * Dependent on the provided output dimension `n`,
       * the result is made more applicable for further
       * data processing steps
       */
      val output = if (n == 1) {
        /*
         * The dimension of the `label` values is either
         * `multivariate` or `univariate`. In both cases,
         * the value(s) are extracted.
         *
         * It is up to the user to decide whether the
         * label value must be 1- or n-dimensional
         */
        if ($(dimensionality) == "multivariate") {

          val label_udf = udf(
            (values:mutable.WrappedArray[mutable.WrappedArray[Double]]) => values.head)
          result.withColumn($(labelCol), label_udf(col($(labelCol))))

        } else {

          val label_udf = udf((vector:Vector) => vector.toArray.head)
          result.withColumn($(labelCol), label_udf(col($(labelCol))))

        }

      } else result

      output

    } else {

      /*
       * Specify a window with lag values as we only want to
       * take feature vector (lag) into account
       */
      val spec = if ($(laggingType) == "pastFeatures")
      /*
       * In this case, we exclude the current point in time;
       * this is implemented to build testsets for model
       * evaluation without data leakage
       */
        Window.partitionBy().rowsBetween(-m, -1)

      else
      /*
      * This case is for predicting purposes to retrieve
      * the next point in time that follows the current
      * one
      */
        Window.partitionBy().rowsBetween(-(m-1), 0)

      val vectorized =
        timeset.withColumn("_vector", collect_list(col($(valueCol))).over(spec))
      /*
       * Select user defined function to compute features
       * for either `multivariate` or `univariate`datasets
       */
      val featuresUDF =
        if ($(dimensionality) == "multivariate")
          multivariateFeatures(m)

        else
          univariateFeatures(m)

      val result = vectorized        /*
         * Build features and remove all null values; null indicates
         * that the length of the feature vector is smaller than 'lag'
         */
        .withColumn($(featuresCol), featuresUDF(col("_vector"))).filter(col($(featuresCol)).isNotNull)
        /*
         * Remove internal column
         */
        .drop("_vector")

      result

    }
  }
  /*
   * __MOD__
   *
   * Support for multivariate input
   */
  private def univariateFeatures(k:Int) = udf {
    vector:mutable.WrappedArray[Double] => {

      if (vector.size == k) {

        val features = vector.toArray
        Vectors.dense(features)

      }
      else null

    }
  }

  private def multivariateFeatures(k:Int) = udf {
    vector:mutable.WrappedArray[mutable.WrappedArray[Double]] => {

      if (vector.size == k) {

        val features = vector
          .map(values => values)

        features

      }
      else null

    }

  }

  /*
   * __MOD__
   *
   * Support for multivariate input and
   * multi-dimensional output
   */
  private def univariateLabelFeatures(m:Int, n:Int) = udf {
    vector:mutable.WrappedArray[Double] => {

      if (vector.size == m + n) {
        /*
         * The first m values of the provided
         * vector are used as `features
         */
        val features = vector.take(m).toArray
        /*
         * The last n values of the provided
         * vector are used as `label`
         */
        val label = vector.takeRight(n).toArray

        Univariate(features = Vectors.dense(features), label = Vectors.dense(label))

      }
      else
        null

    }

  }

  private def multivariateLabelFeatures(m:Int, n:Int) = udf {
    vector: mutable.WrappedArray[mutable.WrappedArray[Double]] => {

      if (vector.size == m + n) {
        /*
         * The first m values of the provided
         * vector are used as `features
         */
        val features = vector.take(m)
          .map(values => values)
        /*
         * The last n values of the provided
         * vector are used as `label`
         */
        val label = vector.takeRight(n)
          .map(values => values)

        Multivariate(features = features, label = label)

      }
      else
        null

    }
  }

  override def transformSchema(schema:StructType):StructType = {
    schema
  }

  override def copy(extra:ParamMap):Lagging = defaultCopy(extra)

}