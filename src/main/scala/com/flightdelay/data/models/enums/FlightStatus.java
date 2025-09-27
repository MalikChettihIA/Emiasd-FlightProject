package com.flightdelay.data.model.enums

object FlightStatus extends Enumeration {
    type FlightStatus = Value

    val OnTime = Value("ON_TIME")
    val Delayed = Value("DELAYED")
    val Cancelled = Value("CANCELLED")
    val Diverted = Value("DIVERTED")

    def fromString(status: String): Option[FlightStatus] = {
            values.find(_.toString == status.toUpperCase)
    }
}