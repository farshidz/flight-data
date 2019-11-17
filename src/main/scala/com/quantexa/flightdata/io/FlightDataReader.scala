package com.quantexa.flightdata.io

import com.quantexa.flightdata.FlightData
import org.apache.spark.sql.Dataset

/**
  * A [[DatasetReader]] that reads a strongly typed [[Dataset]] of [[FlightData]].
  */
class FlightDataReader extends DatasetReader[FlightData]{
  /**
    * Loads a [[Dataset]] of [[FlightData]].
    */
  override def getDataset: Dataset[FlightData] = ???
}
