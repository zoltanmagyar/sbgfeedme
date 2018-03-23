package sbgfeedme.consumer

import org.json4s.CustomSerializer
import org.json4s.JsonAST.JString

import scala.util.Try

/** Type of operation */
sealed trait Operation extends Product {
  val name:String = productPrefix.toLowerCase
}
object Operation {
  def apply(s:String):Operation = s.toLowerCase match {
    case Create.name => Create
    case Update.name => Update
    case _ => throw new IllegalArgumentException(s"Unknown operation: $s")
  }
}
class OperationSerializer extends CustomSerializer[Operation](_=> ({
  case jString:JString => Operation(jString.s)
}, {
  case operation:Operation => JString(operation.name)
}))
/** Create operation */
case object Create extends Operation
/** Update operation */
case object Update extends Operation
/** Represents a message produced by the live feed */
sealed trait Message {
  /** Sequence ID */
  def msgId:Long
  /** Type of operation */
  def operation:Operation
  /** Type of event */
  def `type`:String
  /** Timestamp in milliseconds */
  def timestamp:Long
}
object Message {
  /** Parses a raw string message as produced by the upstream feed into a Message. Returns None if unsuccessful */
  def apply(rawMessage:String):Option[Message] = {
    val delimiter = "|"
    rawMessage.stripPrefix(delimiter).split(s"(?<!\\\\)\\$delimiter") match {
      case Array(msgId, operation, mType, timestamp, eventId, category, subCategory, name, startTime, displayed, suspended) if mType == "event" =>
        Try(Event(msgId.toLong, Operation(operation), mType, timestamp.toLong, eventId, category, subCategory, name,
          startTime.toLong, displayed.toInt==1, suspended.toInt==1)).toOption
      case Array(msgId, operation, mType, timestamp, eventId, marketId, name, displayed, suspended) if mType == "market" =>
        Try(Market(msgId.toLong, Operation(operation), mType, timestamp.toLong, eventId, marketId, name, displayed.toInt==1,
          suspended.toInt==1)).toOption
      case Array(msgId, operation, mType, timestamp, marketId, outcomeId, name, price, displayed, suspended) if mType == "outcome" =>
        Try(Outcome(msgId.toLong, Operation(operation), mType, timestamp.toLong, marketId, outcomeId, name, price,
          displayed.toInt==1, suspended.toInt==1)).toOption
      case _ =>
        System.err.println(s"Couldn't parse line:\n$rawMessage")
        None
    }
  }
}
/** Event data type */
case class Event(msgId:Long, operation:Operation, `type`:String, timestamp:Long, eventId:String, category:String,
                 subCategory:String, name:String, startTime:Long, displayed:Boolean, suspended:Boolean) extends Message
/** Market data type */
case class Market(msgId:Long, operation:Operation, `type`:String, timestamp:Long, eventId:String, marketId:String,
                  name:String, displayed:Boolean, suspended:Boolean) extends Message
/** Outcome data type */
case class Outcome(msgId:Long, operation:Operation, `type`:String, timestamp:Long, marketId:String, outcomeId:String,
                   name:String, price:String, displayed:Boolean, suspended:Boolean) extends Message
