package de.kp.works.spark.ml.text
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
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

trait DocumentizerParams extends Params {

  final val separator = new Param[String](this, "separator",
    "Field value separator", (value:String) => true)

  setDefault(separator -> "=")

  final val documentCol = new Param[String](this, "documentCol",
    "Name of the document (output) field", (value:String) => true)

  setDefault(documentCol -> "document")

  final val featureCols = new Param[Seq[String]](DocumentizerParams.this, "featureCols",
    "Names of the feature fields", (value:Seq[String]) => true)

}
/**
 * This class bridges between numeric and document
 * based data processing; the values of the provided
 * feature columns are serialized and transformed
 * into a "word" representation and aggregated into
 * a "text document".
 *
 * This prepare ground for a variety of text processing
 * models like Latent Dirichlet Allocation, Word2Vec and
 * other natural language processing measures.
 */
class Documentizer(override val uid: String)
  extends Transformer with DocumentizerParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("documentizer"))

  def setDocumentCol(value:String): Documentizer.this.type = set(documentCol, value)

  def setFeatureCols(value:Seq[String]): Documentizer.this.type = set(featureCols, value)

  def setSeparator(value:String): Documentizer.this.type = set(separator, value)

  override def transform(dataset: Dataset[_]): DataFrame = {

    transformSchema(dataset.schema, logging = true)

    def documentizer(featureCols:Seq[String], separator:String) = udf{row:Row => {

      val schema = row.schema
      featureCols.map(featureName => {

        val featureValue = row.get(schema.fieldIndex(featureName))
        val featureText = if (featureValue == null) "null" else featureValue.toString

        val word = s"$featureName$separator$featureText"
        word

      }).toArray

    }}

    val colstruct = struct($(featureCols).map(col): _*)
    dataset.withColumn($(documentCol), documentizer($(featureCols), $(separator))(colstruct))

  }

  override def transformSchema(schema:StructType):StructType = {

   val dataTypes = $(featureCols).map(name => schema(name).dataType)
    dataTypes.foreach {
      case BooleanType   =>
      case ByteType      =>
      case DateType      =>
      case DoubleType    =>
      case FloatType     =>
      case IntegerType   =>
      case LongType      =>
      case ShortType     =>
      case StringType    =>
      case TimestampType =>
      case other => throw new IllegalArgumentException(s"Data type $other is not supported.")
    }

    schema

  }

  override def copy(extra: ParamMap): Documentizer = defaultCopy(extra)

}

object Documentizer extends DefaultParamsReadable[Documentizer] {
  override def load(path:String):Documentizer = super.load(path)
}
