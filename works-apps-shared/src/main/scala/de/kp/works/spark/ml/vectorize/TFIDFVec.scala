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

import de.kp.works.spark.ml.text.Documentizer
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.{HashingTF, IDF}
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

trait TFIDFVecParams extends Params {

  final val featureCols = new Param[Array[String]](this, "featureCols",
    "the list of columns that contain categorical values.", (_:Array[String]) => true)

  final val vectorCol = new Param[String](this, "vectorCol",
    "the name of the output column.", (_:String) => true)

}

class TFIDFVec(override val uid: String) extends Transformer with TFIDFVecParams {

  def this() = this(Identifiable.randomUID("TFIDFVec"))

  def setFeatureCols(value: Array[String]): this.type = set(featureCols, value)

  def setVectorCol(value: String): this.type = set(vectorCol, value)

  def transform(dataset:Dataset[_]):DataFrame = {
    /*
     * Transform provided feature columns into a document
     * representation to enable TFIDF processing.
     */
    val documentizer = new Documentizer()
      .setFeatureCols($(featureCols)).setDocumentCol("_document")

    val documents = documentizer.transform(dataset)
    /*
     * Apply TF/IDF algorithm
     *
     * The Hashing TF and IDF transformers do not maintain a one-on-one
     * mapping with a term. There is no built-in way to correlate a TF-IDF
     * value with the term that produced it (backtracking).
     *
     * Consequence: We can leverage the frequency encoding of the field-value
     * tuples (TF/IDF) to apply clustering and learn those records that have
     * a similar frequency encoding of their values.
     *
     * However, it makes no sense to build a classifier from these vectors
     * as we do not know whether the frequency encoding within a stream batch
     * can be seriously compared to the learn ones.
     */
    val hashingTF = new HashingTF()
      .setInputCol("_document").setOutputCol("_features")

    val features = hashingTF.transform(documents)

    val idf = new IDF()
      .setInputCol("_features").setOutputCol($(vectorCol)).setMinDocFreq(1)

    val model = idf.fit(features)

    val dropCols = Seq("_document", "_features")
    val vectorized = model.transform(features).drop(dropCols: _*)

    vectorized

  }

  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): TFIDFVec = defaultCopy(extra)

}
