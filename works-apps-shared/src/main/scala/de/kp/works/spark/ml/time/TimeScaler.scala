package de.kp.works.spark.ml.time

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
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.stat.Summarizer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.mutable
/**
 * Note: The [[TimeScaler]] from Apache Spark ML does
 * not support inverse scaling operations. 
 * 
 * Therefore, this version is introduced to map input
 * values to the interval from [0, 1] and back.
 */
class TimeScaler {
  
  private var dimensionality:String = "univariate"
  /*
   * Support for univariate datasets
   */
  private var minval:Double = Double.NaN
  private var maxval:Double = Double.NaN
  /*
   * Support for multivariate datasets
   */
  private var minvec:Vector = null
  private var maxvec:Vector = null

  private var forwardInputCol:String = null
  private var scaledCol:String = null

  private var backwardInputCol:String = null
  private var rescaledCol:String = null
  
  def setForwardInputCol(name:String):TimeScaler = {
    forwardInputCol = name
    scaledCol = s"${forwardInputCol}_scaled"
    this
  }
  
  def getForwardOutputCol: String = scaledCol
  
  def setBackwardInputCol(name:String):TimeScaler = {
    backwardInputCol = name
    rescaledCol = s"${backwardInputCol}_rescaled"
    this
  }
  
  def setDimensionality(value:String):TimeScaler = {
    dimensionality = value
    this
  }
  
  def getBackwardOutputCol: String = rescaledCol
  
  def backwardTransform(datasource:DataFrame):DataFrame = {
    
    if (dimensionality == "multivariate") {
      
      if (minvec == null || maxvec == null)
        throw new Exception(s"There do not exist minimum and maximum values.")
 
      def inverse_scaler_udf(minvec:Vector, maxvec:Vector) = udf((values:mutable.WrappedArray[Double]) => {
        
        val rescaled = values.toArray
        values.indices.foreach(i => {
          
           rescaled(i) = try {
            
            val band = maxvec(i) - minvec(i)
            minvec(i) + values(i) * band
            
          } catch {
            case _:Throwable => Double.NaN
          }
          
        })
        
        rescaled.toSeq
        
      })
     
      val inverse_scaler = inverse_scaler_udf(minvec, maxvec)
      val rescaled = datasource.withColumn(rescaledCol, inverse_scaler(col(backwardInputCol)))
      
      rescaled
 
    } else {
      
      if (minval == Double.NaN || maxval == Double.NaN)
        throw new Exception(s"There do not exist minimum and maximum values.")
  
      def inverse_scaler_udf(minval:Double, maxval:Double) = udf((scaled:Double) => {
        try {
          
          val band = maxval - minval
          minval + scaled * band
          
        } catch {
          case _:Throwable => Double.NaN
        }
      })
     
      val inverse_scaler = inverse_scaler_udf(minval, maxval)
      val rescaled = datasource.withColumn(rescaledCol, inverse_scaler(col(backwardInputCol)))
      
      rescaled
    
    }
  }
  
  def forwardTransform(datasource:DataFrame):DataFrame = {

    if (dimensionality == "multivariate") {
      /*
       * STEP #1: Retrieve minimum and maximum vectors
       * from the modified datasource. Modification is
       * necessary as the `Summarizer` demands for a
       * [Vector] column
       */
      val vectorizer = udf((values:mutable.WrappedArray[Double]) =>
        Vectors.dense(values.toArray)
      )
      
      val scaleset = datasource.withColumn(forwardInputCol, vectorizer(col(forwardInputCol)))
      
      val summarizerCol = Summarizer
        .metrics("max", "min")
        .summary(col(forwardInputCol)).as("summary")
      
      val summary = scaleset
        .select(summarizerCol)
        .select("summary.max", "summary.min")
        .first
        
      maxvec = summary.getAs[Vector](0)
      minvec = summary.getAs[Vector](1)
      /*
       * STEP #2: Each minimum and maximum value is applied
       * to scale each datasource value
       */
      def scaler_udf(minvec:Vector, maxvec:Vector) = udf((values:mutable.WrappedArray[Double]) => {
        
        val scaled = values.toArray
        values.indices.foreach(i => {
          
           scaled(i) = try {
            
            val band = maxvec(i) - minvec(i)
            (values(i) - minvec(i)) / band
            
          } catch {
            case _:Throwable => Double.NaN
          }
          
        })
        
        scaled.toSeq
        
      })
      
      val scaler = scaler_udf(minvec, maxvec)
      val scaled = datasource.withColumn(scaledCol, scaler(col(forwardInputCol)))
  
      scaled
      
    } else {
      
      val minmax = datasource
        .agg(min(forwardInputCol).as("minval"), max(forwardInputCol).as("maxval")).collect.head
        
      minval = minmax.getAs[Double]("minval")
      maxval = minmax.getAs[Double]("maxval")
      
      def scaler_udf(minval:Double, maxval:Double) = udf((value:Double) => {
        try {
          
          val band = maxval - minval
          (value - minval) / band
          
        } catch {
          case _:Throwable => Double.NaN
        }
      })
      
      val scaler = scaler_udf(minval, maxval)
      val scaled = datasource.withColumn(scaledCol, scaler(col(forwardInputCol)))
  
      scaled
      
    }
    
  }

}