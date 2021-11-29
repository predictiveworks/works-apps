package de.kp.works.apps.anon.model.ml.cluster

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
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.evaluation._
import org.apache.spark.ml.linalg.{DenseVector, Vector, Vectors}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.{SparkContext, WrappedBLAS, WrappedUtils}

trait ClusteringEvaluatorParams extends Params {

  final val vectorCol = new Param[String](ClusteringEvaluatorParams.this, "vectorCol",
    "The name of the features column", (value: String) => true)

  final val predictionCol = new Param[String](ClusteringEvaluatorParams.this, "predictionCol",
    "The name of the prediction column", (value: String) => true)

  /**
   * param for metric name in evaluation (supports `"silhouette"` (default))
   * @group param
   */
  val metricName: Param[String] = {
    val allowedParams = ParamValidators.inArray(Array("silhouette"))
    new Param(
      this, "metricName", "metric name in evaluation (silhouette)", allowedParams)
  }

  /** @group getParam */
  def getMetricName: String = $(metricName)
  /** @group getParam */
  def getPredictionCol: String = $(predictionCol)

  /** @group getParam */
  def getVectorCol: String = $(vectorCol)

}
/**
 *
 * __KUP__
 *
 * [ClusteringEvaluator] is a downgrading of [ClusterEvaluator] available in v2.3.0
 * for Apache Spark v2.1.3
 *
 * The metric computes the Silhouette measure using the specified distance measure.
 *
 * The Silhouette is a measure for the validation of the consistency within clusters. It ranges
 * between 1 and -1, where a value close to 1 means that the points in a cluster are close to the
 * other points in the same cluster and far from the points of the other clusters.
 */
class ClusteringEvaluator(override val uid: String) extends Evaluator with ClusteringEvaluatorParams {

  def this() = this(Identifiable.randomUID("kmeansEvaluator"))

  override def copy(pMap: ParamMap): ClusteringEvaluator = this.defaultCopy(pMap)

  override def isLargerBetter: Boolean = true

  /** @group setParam */
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  def setVectorCol(value: String): this.type = set(vectorCol, value)

  /** @group setParam */
  def setMetricName(value: String): this.type = set(metricName, value)

  /**
   * param for distance measure to be used in evaluation
   * (supports `"squaredEuclidean"` (default), `"cosine"`)
   * @group param
   */
  val distanceMeasure: Param[String] = {
    val availableValues = Array("squaredEuclidean", "cosine")
    val allowedParams = ParamValidators.inArray(availableValues)
    new Param(this, "distanceMeasure", "distance measure in evaluation. Supported options: " +
      availableValues.mkString("'", "', '", "'"), allowedParams)
  }

  /** @group getParam */
  def getDistanceMeasure: String = $(distanceMeasure)

  /** @group setParam */
  def setDistanceMeasure(value: String): this.type = set(distanceMeasure, value)

  setDefault(metricName -> "silhouette", distanceMeasure -> "squaredEuclidean")

  override def evaluate(dataset: Dataset[_]): Double = {
    /*
     * Limit dataset to those columns that are relevant
     * for evaluation purposes
     */
    val probeset = dataset.select(col($(predictionCol)), col($(vectorCol)))

    ($(metricName), $(distanceMeasure)) match {

      case ("silhouette", "squaredEuclidean") =>
        SquaredEuclideanSilhouette.computeSilhouetteScore(probeset, $(predictionCol), $(vectorCol))

      case ("silhouette", "cosine") =>
        CosineSilhouette.computeSilhouetteScore(probeset, $(predictionCol), $(vectorCol))

      case (mn, dm) =>
        throw new IllegalArgumentException(s"No support for metric $mn, distance $dm")
    }

  }

  override def toString: String = {
    s"ClusteringEvaluator: uid=$uid, metricName=${$(metricName)}, " +
      s"distanceMeasure=${$(distanceMeasure)}"
  }

}

object ClusteringEvaluator extends DefaultParamsReadable[ClusteringEvaluator] {

  override def load(path: String): ClusteringEvaluator = super.load(path)

}

abstract class Silhouette {

  /**
   * It computes the Silhouette coefficient for a point.
   */
  def pointSilhouetteCoefficient(
                                  clusterIds: Set[Double],
                                  pointClusterId: Double,
                                  pointClusterNumOfPoints: Long,
                                  averageDistanceToCluster: Double => Double): Double = {

    if (pointClusterNumOfPoints == 1) {
      /* Single-element clusters have silhouette 0 */
      0.0

    } else {
      /*
       * Here we compute the average dissimilarity of the current
       * point to any cluster of which the point is not a member.
       * The cluster with the lowest average dissimilarity - i.e.
       * the nearest cluster to the current  point - is said to be
       * the "neighboring cluster".
      */
      val otherClusterIds = clusterIds.filter(_ != pointClusterId)
      val neighboringClusterDissimilarity = otherClusterIds.map(averageDistanceToCluster).min
      /*
       * Adjustment for excluding the node itself from the computation
       * of the average dissimilarity
       */
      val currentClusterDissimilarity =
        averageDistanceToCluster(pointClusterId) * pointClusterNumOfPoints /
          (pointClusterNumOfPoints - 1)
      if (currentClusterDissimilarity < neighboringClusterDissimilarity) {
        1 - (currentClusterDissimilarity / neighboringClusterDissimilarity)
      } else if (currentClusterDissimilarity > neighboringClusterDissimilarity) {
        (neighboringClusterDissimilarity / currentClusterDissimilarity) - 1
      } else {
        0.0
      }
    }
  }

  /**
   * Compute the mean Silhouette values of all samples.
   */
  def overallScore(df: DataFrame, scoreColumn: Column): Double = {
    df.select(avg(scoreColumn)).collect()(0).getDouble(0)
  }

}
object SquaredEuclideanSilhouette extends Silhouette {

  private[this] var kryoRegistrationPerformed: Boolean = false

  /**
   * This method registers the class [SquaredEuclideanSilhouette.ClusterStats]
   * for kryo serialization.
   */
  def registerKryoClasses(sc: SparkContext): Unit = {
    if (!kryoRegistrationPerformed) {
      sc.getConf.registerKryoClasses(
        Array(
          classOf[SquaredEuclideanSilhouette.ClusterStats]))
      kryoRegistrationPerformed = true
    }
  }

  case class ClusterStats(featureSum: Vector, squaredNormSum: Double, numOfPoints: Long)

  /**
   * The method takes the input dataset and computes the aggregated values
   * about a cluster which are needed by the algorithm.
   */
  def computeClusterStats(df: DataFrame, predictionCol: String, vectorCol: String): Map[Double, ClusterStats] = {

    val numFeatures = WrappedUtils.getNumFeatures(df, vectorCol)

    val clustersStatsRDD = df.select(
      col(predictionCol).cast(DoubleType), col(vectorCol), col("squaredNorm"))
      .rdd
      .map { row => (row.getDouble(0), (row.getAs[Vector](1), row.getDouble(2))) }
      .aggregateByKey[(DenseVector, Double, Long)]((Vectors.zeros(numFeatures).toDense, 0.0, 0L))(
        seqOp = {
          case (
            (featureSum: DenseVector, squaredNormSum: Double, numOfPoints: Long),
            (features, squaredNorm)
            ) =>
            WrappedBLAS.axpy(1.0, features, featureSum)
            (featureSum, squaredNormSum + squaredNorm, numOfPoints + 1)
        },
        combOp = {
          case (
            (featureSum1, squaredNormSum1, numOfPoints1),
            (featureSum2, squaredNormSum2, numOfPoints2)
            ) =>
            WrappedBLAS.axpy(1.0, featureSum2, featureSum1)
            (featureSum1, squaredNormSum1 + squaredNormSum2, numOfPoints1 + numOfPoints2)
        })

    clustersStatsRDD
      .collectAsMap()
      .mapValues {
        case (featureSum: DenseVector, squaredNormSum: Double, numOfPoints: Long) =>
          SquaredEuclideanSilhouette.ClusterStats(featureSum, squaredNormSum, numOfPoints)
      }
      .toMap
  }

  /**
   * It computes the Silhouette coefficient for a point.
   */
  def computeSilhouetteCoefficient(
                                    broadcastedClustersMap: Broadcast[Map[Double, ClusterStats]],
                                    point: Vector,
                                    clusterId: Double,
                                    squaredNorm: Double): Double = {

    def compute(targetClusterId: Double): Double = {
      val clusterStats = broadcastedClustersMap.value(targetClusterId)
      val pointDotClusterFeaturesSum = WrappedBLAS.dot(point, clusterStats.featureSum)

      squaredNorm +
        clusterStats.squaredNormSum / clusterStats.numOfPoints -
        2 * pointDotClusterFeaturesSum / clusterStats.numOfPoints
    }

    pointSilhouetteCoefficient(
      broadcastedClustersMap.value.keySet,
      clusterId,
      broadcastedClustersMap.value(clusterId).numOfPoints,
      compute)
  }

  /**
   * Compute the Silhouette score of the dataset using squared Euclidean distance measure.
   */
  def computeSilhouetteScore(dataset: Dataset[_],  predictionCol: String, vectorCol: String): Double = {
    SquaredEuclideanSilhouette.registerKryoClasses(dataset.sparkSession.sparkContext)

    val squaredNormUDF = udf {
      features: Vector => math.pow(Vectors.norm(features, 2.0), 2.0)
    }

    val dfWithSquaredNorm = dataset.withColumn("squaredNorm", squaredNormUDF(col(vectorCol)))

    /* compute aggregate values for clusters needed by the algorithm */
    val clustersStatsMap = SquaredEuclideanSilhouette
      .computeClusterStats(dfWithSquaredNorm, predictionCol, vectorCol)

    // Silhouette is reasonable only when the number of clusters is greater then 1
    assert(clustersStatsMap.size > 1, "Number of clusters must be greater than one.")

    val bClustersStatsMap = dataset.sparkSession.sparkContext.broadcast(clustersStatsMap)

    val computeSilhouetteCoefficientUDF = udf {
      computeSilhouetteCoefficient(bClustersStatsMap, _: Vector, _: Double, _: Double)
    }

    val silhouetteScore = overallScore(
      dfWithSquaredNorm,
      computeSilhouetteCoefficientUDF(col(vectorCol), col(predictionCol).cast(DoubleType),
        col("squaredNorm")))

    bClustersStatsMap.destroy()

    silhouetteScore
  }
}

object CosineSilhouette extends Silhouette {

  private[this] val normalizedFeaturesColName = "normalizedFeatures"

  /**
   * The method takes the input dataset and computes the aggregated values
   * about a cluster which are needed by the algorithm.
   */
  def computeClusterStats(df: DataFrame, vectorCol: String, predictionCol: String): Map[Double, (Vector, Long)] = {

    val numFeatures = WrappedUtils.getNumFeatures(df, vectorCol)

    val clustersStatsRDD = df.select(
      col(predictionCol).cast(DoubleType), col(normalizedFeaturesColName))
      .rdd
      .map { row => (row.getDouble(0), row.getAs[Vector](1)) }
      .aggregateByKey[(DenseVector, Long)]((Vectors.zeros(numFeatures).toDense, 0L))(
        seqOp = {
          case ((normalizedFeaturesSum: DenseVector, numOfPoints: Long), normalizedFeatures) =>
            WrappedBLAS.axpy(1.0, normalizedFeatures, normalizedFeaturesSum)
            (normalizedFeaturesSum, numOfPoints + 1)
        },
        combOp = {
          case ((normalizedFeaturesSum1, numOfPoints1), (normalizedFeaturesSum2, numOfPoints2)) =>
            WrappedBLAS.axpy(1.0, normalizedFeaturesSum2, normalizedFeaturesSum1)
            (normalizedFeaturesSum1, numOfPoints1 + numOfPoints2)
        })

    clustersStatsRDD
      .collectAsMap()
      .toMap
  }

  def computeSilhouetteCoefficient(
    broadcastedClustersMap: Broadcast[Map[Double, (Vector, Long)]],
    normalizedFeatures: Vector,
    clusterId: Double): Double = {

    def compute(targetClusterId: Double): Double = {
      val (normalizedFeatureSum, numOfPoints) = broadcastedClustersMap.value(targetClusterId)
      1 - WrappedBLAS.dot(normalizedFeatures, normalizedFeatureSum) / numOfPoints
    }

    pointSilhouetteCoefficient(
      broadcastedClustersMap.value.keySet,
      clusterId,
      broadcastedClustersMap.value(clusterId)._2,
      compute)
  }

  /**
   * Compute the Silhouette score of the dataset using the cosine distance measure.
   */
  def computeSilhouetteScore(dataset: Dataset[_], predictionCol: String, vectorCol: String): Double = {

    val normalizeFeatureUDF = udf {
      features: Vector =>
      {
        val norm = Vectors.norm(features, 2.0)
        WrappedBLAS.scal(1.0 / norm, features)
        features
      }
    }

    val dfWithNormalizedFeatures = dataset.withColumn(
      normalizedFeaturesColName,
      normalizeFeatureUDF(col(vectorCol)))

    /* compute aggregate values for clusters needed by the algorithm */
    val clustersStatsMap = computeClusterStats(dfWithNormalizedFeatures, vectorCol, predictionCol)

    /* Silhouette is reasonable only when the number of clusters is greater then 1 */
    assert(clustersStatsMap.size > 1, "Number of clusters must be greater than one.")

    val bClustersStatsMap = dataset.sparkSession.sparkContext.broadcast(clustersStatsMap)

    val computeSilhouetteCoefficientUDF = udf {
      computeSilhouetteCoefficient(bClustersStatsMap, _: Vector, _: Double)
    }

    val silhouetteScore = overallScore(
      dfWithNormalizedFeatures,
      computeSilhouetteCoefficientUDF(
        col(normalizedFeaturesColName),
        col(predictionCol).cast(DoubleType)))

    bClustersStatsMap.destroy()

    silhouetteScore
  }

}