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

/**
 * [[TimeDiffScaler]] is a 2-step transformer for non-stationary
 * timeseries. First, 1st or 2nd order differentiation is applied
 * to make the time series stationary. Second, a Min-Max scaler
 * is applied to map values onto [0, 1]
 */
class TimeDiffScaler {
  
  private var dimensionality:String = "univariate"
  /*
   * timeCol is required by the differentiation stage to 
   * apply lagging
   */
  private var timeCol:String = null
  /*
   * forwardInputCol specifies the column that contains the values
   * for the forward transformation
   */
  private var forwardInputCol:String = null
  /*
   * backwardInputCol specifies the column that contains the values
   * for the forward transformation
   */
  private var backwardInputCol:String = null
  
  private var backwardOutputCol:String = null
  
  private var diffOrder:Int = 0
  private var diffOffset:Int = 0
  
  private val differ = new TimeDiffer()
  private val scaler = new TimeScaler()
  
  def setForwardInputCol(name:String):TimeDiffScaler = {
    forwardInputCol = name
    this
  }
  
  def setBackwardInputCol(name:String):TimeDiffScaler = {
    backwardInputCol = name
    this
  }
  
  def setBackwardOutputCol(name:String):TimeDiffScaler = {
    backwardOutputCol = name
    this
  }
  
  def setTimeCol(name:String):TimeDiffScaler = {
    timeCol = name
    this
  }
  
  def setDiffOrder(value:Int):TimeDiffScaler = {
    diffOrder = value
    this
  }
  
  def setDiffOffset(value:Int):TimeDiffScaler = {
    diffOffset = value
    this
  }
  
  def setDimensionality(value:String):TimeDiffScaler = {
    dimensionality = value
    this
  }
  /*
   * This is the result column of both transformation,
   * i.e. the output column of the scaler
   */
  def getForwardOutputCol: String = scaler.getForwardOutputCol
  
  def transform(datasource:DataFrame, direction:String):DataFrame = {
    
    direction match {
      case "forward"  => forwardTransform(datasource)
      case "backward" => backwardTransform(datasource)
      
      case _ => throw new Exception(s"Direction `$direction` is not supported.")
    }
    
  }
  
  private def forwardTransform(datasource:DataFrame):DataFrame = {
    /*
     * This method supports the use of differentiating
     * and scaling as well as scaling only
     */
    if (diffOrder == 0) {
      /*
       * In this case the provided `forwardInputCol` is
       * directly presented to the scaler component
       */
      scaler
        .setForwardInputCol(forwardInputCol)
        .setDimensionality(dimensionality)
      
      val scaled = scaler.forwardTransform(datasource)
      scaled

    } else {
      /*
       * STEP #1: Differentiation is applied to the 
       * forwardInputCol and the respective lag column
       */
      differ
        .setForwardInputCol(forwardInputCol)
        .setTimeCol(timeCol)
        .setDiffOrder(diffOrder)
        .setDiffOffset(diffOffset)  
        .setDimensionality(dimensionality)
  
      val diffed = differ.forwardTransform(datasource)
      /*
       * STEP #2: Min-Max Scaling is applied to the
       * result of the differentiation and depends
       * on the differentiation order.
       */
      val diffCol = differ.getForwardOutputCol
      scaler
        .setForwardInputCol(diffCol)
        .setDimensionality(dimensionality)
      
      val scaled = scaler.forwardTransform(diffed)
      scaled
      
    }    
  }
  
  private def backwardTransform(datasource:DataFrame):DataFrame = {
    /*
     * This method supports the use of differentiating
     * and scaling as well as scaling only
     */
    if (diffOrder == 0) {      
      /*
       * STEP #1: Min-Max Scaling is applied to the
       * backwardInputCol
       */
      scaler
        .setBackwardInputCol(backwardInputCol)
        .setDimensionality(dimensionality)
        
      val rescaled = scaler
        .backwardTransform(datasource)
        /*
         * Drop backward input column
         */
        .drop(backwardInputCol)
             
      /*
       * STEP #2: The `rescaledCol` is renamed to
       * match the expected output column name
       */
      val rescaledCol = scaler.getBackwardOutputCol
      rescaled.withColumnRenamed(rescaledCol, backwardOutputCol)
        
    } else {      
      /*
       * STEP #1: Min-Max Scaling is applied to the
       * backwardInputCol
       */
      scaler
        .setBackwardInputCol(backwardInputCol)
        .setDimensionality(dimensionality)
        
      val rescaled = scaler
        .backwardTransform(datasource)
        /*
         * Drop backward input column
         */
        .drop(backwardInputCol)
      /*
       * STEP #2: Differentiation is applied to the
       * output column of the scaler
       */
      val rescaledCol = scaler.getBackwardOutputCol
      
      differ
        .setBackwardInputCol(rescaledCol).setBackwardOutputCol(backwardOutputCol)
        .setDimensionality(dimensionality)
      
      val rediffed = differ
        .backwardTransform(rescaled)
        /*
         * Drop backward input column
         */
        .drop(rescaledCol)
        
      rediffed
    
    }
    
  }

}