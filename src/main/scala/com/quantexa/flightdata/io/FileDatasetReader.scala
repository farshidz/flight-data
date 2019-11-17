package com.quantexa.flightdata.io

import org.apache.spark.sql.Dataset

class FileDatasetReader[T] extends DatasetReader[T] {
  override def getDataset: Dataset[T] = ???
}
