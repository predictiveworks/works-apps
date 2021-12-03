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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class TimeRandomizer {
  
  private var inputCol:String  = null
  private var outputCol:String = null
  
  private var withDiff:Boolean = false
  
  def setInputCol(name:String):TimeRandomizer = {
    inputCol = name
    this
  }
  
  def setOutputCol(name:String):TimeRandomizer = {
    outputCol = name
    this
  }
  
  def setWithDiff(value:Boolean):TimeRandomizer = {
    withDiff = value
    this
  }
  
  /*
   * This method transforms the value of a certain 
   */
  def transform(datasource:DataFrame):DataFrame = {
    
    val minmax = datasource
      .agg(min(inputCol).as("minval"), max(inputCol).as("maxval")).collect.head
      
    val minval = minmax.getAs[Double]("minval")
    val maxval = minmax.getAs[Double]("maxval")
    
    val diffval = maxval - minval 
    val random  = scala.util.Random
    
    def randomizer_udf(diffval:Double, random:scala.util.Random) = udf((_:Double) => {

      /* Build random number between [0, 1] */
      val randval = diffval * random.nextDouble()
      randval
      
    })
    
    val randomizer = randomizer_udf(diffval, random)
    
    val randomCol = s"${inputCol}_rand"
    val randomized = datasource.withColumn(randomCol, randomizer(col(inputCol)))
    /*
     * The [TSRandomizer] supports randomization and also 
     * computation of the difference between signal and its
     * randomized counterpart. 
     */
    if (withDiff) {
      randomized.withColumn(outputCol, col(inputCol) - col(randomCol))
      
    } else {
      randomized.withColumnRenamed(randomCol, outputCol)
    }
    
  }
}