package com.quantexa.flightdata.analytics

import java.sql.Date

import com.quantexa.flightdata.{FlightData, Passenger}
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * This class provides methods to calculate various statistics for flight data.
  */
class FlightStatistics(spark: SparkSession) {

  import spark.implicits._

  /**
    * Calculates the number of flights in each calendar month.
    *
    * @param flightData Dataset of [[FlightData]]
    * @param year       The year to calculate results for
    * @return A [[Dataset]] of [[MonthStatistics]]
    */
  def flightsPerMonth(flightData: Dataset[FlightData], year: Int): Dataset[MonthStatistics] = {
    flightData
      .map(r => Flight(r.flightId, r.from, r.to, r.date))
      .dropDuplicates
      .filter(_.date.getYear == year)
      .groupByKey(_.date.getMonth)
      .count
      .map(t => MonthStatistics(t._1, t._2.toInt))
  }

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

case class Flight(flightId: Int,
                  from: String,
                  to: String,
                  date: Date)

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
