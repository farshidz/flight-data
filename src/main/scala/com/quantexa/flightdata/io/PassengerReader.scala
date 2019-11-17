package com.quantexa.flightdata.io

import com.quantexa.flightdata.Passenger
import org.apache.spark.sql.Dataset

/**
  * A [[DatasetReader]] that reads a strongly typed [[Dataset]] of [[Passenger]]s.
  */
class PassengerReader extends DatasetReader [Passenger]{
  /**
    * Loads a [[Dataset]] of [[Passenger]]s.
    */
  override def getDataset: Dataset[Passenger] = ???
}
