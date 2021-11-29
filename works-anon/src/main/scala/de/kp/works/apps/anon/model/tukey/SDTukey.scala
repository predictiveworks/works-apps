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

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

/**
 * 'SDTukey' is a method to identify outlier in statistics.
 * The logic of the algorithm as follows:
 *
 * Let’s say we have Q1 as first quantile(25%) and Q3 as
 * third quantile(75%), the inter quantile range or IQR
 * will be given as IQR = Q3 - Q1.
 *
 * IQR gives the width of distribution of data between 25%
 * and 75% of data. Using IQR we can identify the outliers.
 *
 * This method is known as Box and Whisker method.
 *
 * In this method, any value smaller than Q1 - 1.5 * IQR or
 * any value greater than Q3 + 1.5 * IQR will be identified
 * as outlier.
 */
class SDTukey(override val uid: String) extends Transformer with TukeyParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("SDTukey"))

  /** @group setParam */
  def setValueCol(value: String): this.type = set(valueCol, value)

  override def transform(dataset: Dataset[_]): DataFrame = {

    transformSchema(dataset.schema, logging = true)
    val colname = $(valueCol)

    val field = dataset.schema(colname)
    val ftype = getFieldType(field)

    if (ftype == "categorical")
      throw new IllegalArgumentException("[SDTukey] The provided value field contains categorical values. "
        + "This algorithm requires numerical values only.")

    /*
     * The Tukey mechanism enriches the incoming dataset with an additional
     * parameter, tukey_dist, that can be used to describe the associated
     * values as anomalies
     */
    val distances = Tukey.computeDistance(dataset, field.name)
    distances.toDF()

  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): SDTukey = defaultCopy(extra)

}

object SDTukey extends DefaultParamsReadable[SDTukey] {
  override def load(path: String): SDTukey = super.load(path)
}
