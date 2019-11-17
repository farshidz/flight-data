package com.quantexa.flightdata.io

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.apache.spark.sql.types.StructType

import scala.reflect.runtime.universe.TypeTag

/**
  * A [[DatasetReader]] that loads CSV data from a file system.
  *
  * @param spark  An instance of [[SparkSession]]
  * @param path   Path of the CSV file
  * @param schema Optional schema of the data
  * @tparam T Type of [[Dataset]]. Either a case class or [[org.apache.spark.sql.Row]] for a generic
  *           [[org.apache.spark.sql.DataFrame]]
  */
class CsvDatasetReader[T: Encoder](spark: SparkSession,
                                   path: String,
                                   schema: Option[StructType] = None
                                  ) extends DatasetReader[T] {
  override def getDataset: Dataset[T] = {
    spark
      .read
      .format("csv")
      .option("header", "true")
      .schema(schema.orNull)
      .load(path)
      .as[T]
  }
}
