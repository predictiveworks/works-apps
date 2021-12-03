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
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import scala.collection.mutable

class TimeDiffer {
  
  private var dimensionality:String = "univariate"
  /*
   * timeCol is required by the differentiation stage to 
   * apply lagging
   */
  private var timeCol:String = _
  /*
   * forwardInputCol specifies the column that contains 
   * the values for the forward transformation
   */
  private var forwardInputCol:String = _
  /*
   * backwardInputCol specifies the column that contains 
   * the values for the forward transformation
   */
  private var backwardInputCol:String = _
  /*
   * Intermediate columns for either 1st or 2nd order
   * differentiation
   */
  private var lag1Col:String = _
  private var diff1Col:String = _

  private var lag2Col:String = _
  private var diff2Col:String = _
  /*
   * outputCol specifies the column of inverse
   * differentiation stage   
   */ 
  private var backwardOutputCol:String = _
  
  private var diffOrder:Int = 1
  private var diffOffset:Int = 1
  
  def setForwardInputCol(name:String):TimeDiffer = {
    forwardInputCol = name
    this
  }
  
  def setBackwardInputCol(name:String):TimeDiffer = {
    backwardInputCol = name
    this
  }
  
  def setTimeCol(name:String):TimeDiffer = {
    timeCol = name
    this
  }
  
  def setDiffOrder(value:Int):TimeDiffer = {
    diffOrder = value
    this
  }
  
  def setDiffOffset(value:Int):TimeDiffer = {
    diffOffset = value
    this
  }
  
  def setDimensionality(value:String):TimeDiffer = {
    dimensionality = value
    this
  }

  def setBackwardOutputCol(colname:String):TimeDiffer = {
    backwardOutputCol = colname
    this
  }
  
  def getForwardOutputCol: String = {
    
    if (diffOrder == 1) 
      diff1Col 
    
    else diff2Col

  }
  /*
   * This method supports first- or second order
   * differentiation of the provided time series
   */
  def forwardTransform(datasource:DataFrame):DataFrame = {
  
    require(diffOrder == 1 || diffOrder == 2, "Parameter `diffOrder` must be either 1 or 2")
        
    lag1Col  = s"${forwardInputCol}_lag_$diffOffset"
    diff1Col = s"${forwardInputCol}_diff_1"
      
    /*
     * STEP #1: Transform datasource by applying the `lag`function
     */
    val spec = Window.orderBy(timeCol)
    val lag1 = datasource
      .withColumn(lag1Col, lag(col(forwardInputCol), diffOffset).over(spec))
      /* Make sure the values of the `lagCol` are not null
       */
      .filter(col(lag1Col).isNotNull)
   

    if (dimensionality == "multivariate") {
      
      val diffUDF = udf((v1:mutable.WrappedArray[Double], v2:mutable.WrappedArray[Double]) => {
        
        val d = v1.toArray
        v1.indices
          .foreach(i => d(i) = v1(i) - v2(i))
        
        d.toSeq

      })
      
      val diff1 = lag1
        /*
         * First-order derivative: diff1 = value - lag1 (value).
         * 
         * Inverse operation: value = diff1 + lag1. In order to
         * support inverse operations, we must not drop the lag
         * column
         */
        .withColumn(diff1Col, diffUDF(col(forwardInputCol), col(lag1Col)))
    
      if (diffOrder == 1)
        diff1
  
      else {
        /*
         * STEP #3: Build the second-order derivative
         */
        lag2Col = s"${diff1Col}_lag_$diffOffset"
        diff2Col = s"${forwardInputCol}_diff_2"
        
        val lag2 = diff1
          /*
           * Second-order derivative: diff2 = diff1 - lag2 
           * which is: diff2 = value - lag1 - lag2
           * 
           * Inverse operation: value = diff2 + lag2 + lag1
           */
          .withColumn(lag2Col, lag(col(diff1Col), diffOffset).over(spec))
          /* Make sure the values of the `lagCol` are not null
           */
          .filter(col(lag2Col).isNotNull)
          
         val diff2 = lag2
           .withColumn(diff2Col, diffUDF(col(diff1Col), col(lag2Col)))
   
         diff2
         
      }        
    }
    else {
      /*
       * STEP #2: Build the first-order derivate as this must be
       * computed in all cases
       */
      val diff1 = lag1
        /*
         * First-order derivative: diff1 = value - lag1 (value).
         * 
         * Inverse operation: value = diff1 + lag1. In order to
         * support inverse operations, we must not drop the lag
         * column
         */
        .withColumn(diff1Col, col(forwardInputCol) - col(lag1Col))
    
      if (diffOrder == 1)
        diff1
  
      else {
        /*
         * STEP #3: Build the second-order derivative
         */
        lag2Col = s"${diff1Col}_lag_$diffOffset"
        diff2Col = s"${forwardInputCol}_diff_2"
        
        val lag2 = diff1
          /*
           * Second-order derivative: diff2 = diff1 - lag2 
           * which is: diff2 = value - lag1 - lag2
           * 
           * Inverse operation: value = diff2 + lag2 + lag1
           */
          .withColumn(lag2Col, lag(col(diff1Col), diffOffset).over(spec))
          /* Make sure the values of the `lagCol` are not null
           */
          .filter(col(lag2Col).isNotNull)
  
        val diff2 = lag2
           .withColumn(diff2Col, col(diff1Col) - col(lag2Col))
   
       diff2
       
      }
      
    }
  
  }

  def backwardTransform(datasource:DataFrame):DataFrame = {
    
    require(diffOrder == 1 || diffOrder == 2, "Parameter `diffOrder` must be either 1 or 2")

    val fieldNames = datasource.schema.fieldNames
    
    if (diffOrder == 1) {
      
      if (lag1Col == null)
        throw new Exception(s"The columns required for this operation are not available")
      
      if (dimensionality == "multivariate") {
      
        val sumUDF = udf((v1:mutable.WrappedArray[Double], v2:mutable.WrappedArray[Double]) => {
          
          val s = v1.toArray
          v1.indices
            .foreach(i => s(i) = v1(i) + v2(i))
          
          s.toSeq
  
        })
        
        datasource.withColumn(backwardOutputCol, sumUDF(col(backwardInputCol), col(lag1Col)))

      } else 
        datasource.withColumn(backwardOutputCol, col(backwardInputCol) + col(lag1Col))
      
    } else {
      
      if (lag1Col == null || lag2Col == null)
        throw new Exception(s"The columns required for this operation are not available")
      
      if (dimensionality == "multivariate") {
      
        val sumUDF = udf((v1:mutable.WrappedArray[Double], v2:mutable.WrappedArray[Double], v3:mutable.WrappedArray[Double]) => {
          
          val s = v1.toArray
          v1.indices
            .foreach(i => s(i) = v1(i) + v2(i) + v3(i)) 
          
          s.toSeq
  
        })
        
        datasource.withColumn(backwardOutputCol, sumUDF(col(backwardInputCol), col(lag2Col), col(lag1Col)))
      
      } else
        datasource.withColumn(backwardOutputCol, col(backwardInputCol) + col(lag2Col) + col(lag1Col))
      
    }
  
  }
  
}