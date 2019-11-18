package com.quantexa.flightdata.analytics

import java.sql.Date

import com.quantexa.flightdata.{FlightData, Passenger}
import org.apache.spark.sql.Dataset

/**
  * This class provides methods to calculate various statistics for flight data.
  */
class FlightStatistics {
  /**
    * Calculates the number of flights in each calendar month.
    *
    * @param flightData Dataset of [[FlightData]]
    * @return A [[Dataset]] of [[MonthStatistics]]
    */
  def flightsPerMonth(flightData: Dataset[FlightData]): Dataset[MonthStatistics] = ???

  /**
    * Finds frequent flyers.
    *
    * @param flightData Dataset of [[FlightData]]
    * @param passengers Dataset of [[Passenger]]s
    * @param maxResults Maximum number of frequent flyers to return
    * @return A [[Dataset]] of [[FrequentFlyer]]s
    */
  def frequentFlyers(flightData: Dataset[FlightData],
                     passengers: Dataset[Passenger],
                     maxResults: Int = 100): Dataset[FrequentFlyer] = ???

  /**
    * Calculates the greatest number of countries a passenger has been in without being in a certain country 
    * (reference country).
    *
    * @param flightData Dataset of [[FlightData]]
    * @param passengers Dataset of [[Passenger]]s
    * @param refCountry The reference country
    * @return A [[Dataset]] of [[PassengerStatistics]]
    */
  def longestRuns(flightData: Dataset[FlightData],
                  passengers: Dataset[Passenger],
                  refCountry: String): Dataset[PassengerStatistics] = ???

  /**
    * Finds passengers who have been on multiple flights together.
    *
    * @param flightData Dataset of [[FlightData]]
    * @param passengers Dataset of [[Passenger]]s
    * @param minFlights Minimun number of shared flights to look for
    * @param from       Optional date from
    * @param to         Optional date to
    * @return A [[Dataset]] of [[PassengerPairStatistics]]
    */
  def sharedFlights(flightData: Dataset[FlightData],
                    passengers: Dataset[Passenger],
                    minFlights: Int,
                    from: Option[Date] = None,
                    to: Option[Date] = None): Dataset[PassengerPairStatistics] = ???
}

case class MonthStatistics(month: Int,
                           noFlights: Int)

case class FrequentFlyer(passengerId: Int,
                         firstName: String,
                         lastName: String,
                         noFlights: Int)

case class PassengerStatistics(passengerId: Int,
                               longestRun: Int)

case class PassengerPairStatistics(passengerId1: Int,
                                   passengerId2: Int,
                                   noFlights: Int,
                                   from: Option[Date] = None,
                                   to: Option[Date] = None)
