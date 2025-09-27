package com.flightdelay.data.models

case class Flight(
   flDate: String,
   opCarrierAirlineId: Int,
   opCarrierFlNum: Int,
   originAirportId: Int,
   destAirportId: Int,
   crsDepTime: Int,
   arrDelayNew: Option[Double],
   canceled: Int,
   diverted: Int,
   crsElapsedTime: Option[Double],
   weatherDelay: Option[Double],
   nasDelay: Option[Double]
)
