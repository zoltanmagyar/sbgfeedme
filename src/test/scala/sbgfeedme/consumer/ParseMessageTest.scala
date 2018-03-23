package sbgfeedme.consumer

import org.scalatest.FunSuite

class ParseMessageTest extends FunSuite {
  test("|| fails the parser") {
    val message = Message("||")
    assert(message.isEmpty)
  }
  test("create event message") {
    val s = "|912|create|event|1516745010279|fac7b8b1-36ae-4a87-9e36-6ef73b59087f|Football|Premier League|\\|Tottenham\\| vs \\|Manchester Utd\\||1516744680662|0|1|"
    val message = Message(s)
    assert(message.get.operation == Create)
    assert(message.collect{case event:Event=>event}.isDefined)
  }
  test("create market message") {
    val s = "|913|create|market|1516745010279|fac7b8b1-36ae-4a87-9e36-6ef73b59087f|d9767467-ef64-4ff3-92b4-d29cc1f369f2|Full Time Result|0|1|"
    val message = Message(s)
    assert(message.get.operation == Create)
    assert(message.collect{case market:Market=>market}.isDefined)
  }
  test("create outcome message") {
    val s = "|914|create|outcome|1516745010279|d9767467-ef64-4ff3-92b4-d29cc1f369f2|b1506898-18c7-407b-b14a-9de596e58763|\\|Tottenham\\||5/2|0|1|"
    val message = Message(s)
    assert(message.get.operation == Create)
    assert(message.collect{case outcome:Outcome=>outcome}.isDefined)
  }
  test("update event message") {
    val s = "|950|update|event|1516745010280|fb57133e-bc2a-4e45-9e44-e4f5dbf2785d|Football|Sky Bet League One|\\|Doncaster\\| vs \\|Portsmouth\\||1516744675646|1|0|"
    val message = Message(s)
    assert(message.get.operation == Update)
    assert(message.collect{case event:Event=>event}.isDefined)
  }
  test("update market message") {
    val s = "|951|update|market|1516745010280|fb57133e-bc2a-4e45-9e44-e4f5dbf2785d|ba9745b1-28c4-4532-b1da-908aec4d0f41|Full Time Result|1|0|"
    val message = Message(s)
    assert(message.get.operation == Update)
    assert(message.collect{case market:Market=>market}.isDefined)
  }
  test("update outcome message") {
    val s = "|952|update|outcome|1516745010280|ba9745b1-28c4-4532-b1da-908aec4d0f41|dd602197-65e3-4e67-8130-d9d7feda41dc|\\|Doncaster\\||1/2|1|0|"
    val message = Message(s)
    assert(message.get.operation == Update)
    assert(message.collect{case outcome:Outcome=>outcome}.isDefined)
  }
}
