package sbgfeedme.consumer

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class AggregationStageTest extends FunSuite with BeforeAndAfterAll {
  implicit val system:ActorSystem = ActorSystem()
  implicit val materializer:ActorMaterializer = ActorMaterializer()

  test("create operations for an event are aggregated") {
    val (pub, sub) = TestSource.probe[Message]
      .via(new AggregationStage)
      .toMat(TestSink.probe[Aggregate])(Keep.both)
      .run()

    sub.request(n=2)
    val event = Message("|912|create|event|1516745010279|fac7b8b1-36ae-4a87-9e36-6ef73b59087f|Football|Premier League|\\|Tottenham\\| vs \\|Manchester Utd\\||1516744680662|0|1|")
      .collect{case event:Event=>event}.get
    val market1 = Message("|913|create|market|1516745010279|fac7b8b1-36ae-4a87-9e36-6ef73b59087f|d9767467-ef64-4ff3-92b4-d29cc1f369f2|Full Time Result|0|1|")
      .collect{case market:Market=>market}.get
    val market1outcome1 = Message("|914|create|outcome|1516745010279|d9767467-ef64-4ff3-92b4-d29cc1f369f2|b1506898-18c7-407b-b14a-9de596e58763|\\|Tottenham\\||5/2|0|1|")
      .collect{case outcome:Outcome=>outcome}.get
    val market1outcome2 = Message("|915|create|outcome|1516745010279|d9767467-ef64-4ff3-92b4-d29cc1f369f2|5edb7339-e511-445f-a1c6-6516cd28a903|Draw|11/10|0|1|")
      .collect{case outcome:Outcome=>outcome}.get
    val market1outcome3 = Message("|916|create|outcome|1516745010279|d9767467-ef64-4ff3-92b4-d29cc1f369f2|37d75063-896c-4538-a3a9-0c5157dc3943|\\|Manchester Utd\\||1/16|0|1|")
      .collect{case outcome:Outcome=>outcome}.get
    val market2 = Message("|917|create|market|1516745010279|fac7b8b1-36ae-4a87-9e36-6ef73b59087f|f17163b2-211a-49ca-ae97-97a384a6ee71|Both Teams To Score|0|1|")
      .collect{case market:Market=>market}.get
    val market2outcome1 = Message("|918|create|outcome|1516745010279|f17163b2-211a-49ca-ae97-97a384a6ee71|5b3325df-0dc7-4415-97cc-6bee94452a08|Yes|25/1|0|1|")
      .collect{case outcome:Outcome=>outcome}.get
    val market2outcome2 = Message("|919|create|outcome|1516745010279|f17163b2-211a-49ca-ae97-97a384a6ee71|fb336542-b0d9-4282-8f96-2a55cde6e5c2|No|8/1|0|1|")
      .collect{case outcome:Outcome=>outcome}.get
    val update = Message("|950|update|event|1516745010280|fb57133e-bc2a-4e45-9e44-e4f5dbf2785d|Football|Sky Bet League One|\\|Doncaster\\| vs \\|Portsmouth\\||1516744675646|1|0|")
      .collect{case update:Event=>update}.get
    pub.sendNext(event)
    pub.sendNext(market1)
    pub.sendNext(market1outcome1)
    pub.sendNext(market1outcome2)
    pub.sendNext(market1outcome3)
    pub.sendNext(market2)
    pub.sendNext(market2outcome1)
    pub.sendNext(market2outcome2)
    pub.sendNext(update)
    pub.sendComplete()

    sub.expectNext(CreateEventData(event, List(
      CreateMarketData(market2, List(market2outcome2, market2outcome1)),
      CreateMarketData(market1, List(market1outcome3, market1outcome2, market1outcome1)))))
    sub.expectNext(UpdateEventData(update))
    sub.expectComplete()
  }

  test("update operations are not aggregated") {
    val (pub, sub) = TestSource.probe[Message]
      .via(new AggregationStage)
      .toMat(TestSink.probe[Aggregate])(Keep.both)
      .run()

    sub.request(n=3)
    val updateEvent = Message("|950|update|event|1516745010280|fb57133e-bc2a-4e45-9e44-e4f5dbf2785d|Football|Sky Bet League One|\\|Doncaster\\| vs \\|Portsmouth\\||1516744675646|1|0|")
      .collect{case update:Event=>update}.get
    val updateMarket = Message("|951|update|market|1516745010280|fb57133e-bc2a-4e45-9e44-e4f5dbf2785d|ba9745b1-28c4-4532-b1da-908aec4d0f41|Full Time Result|1|0|")
      .collect{case update:Market=>update}.get
    val updateOutcome = Message("|952|update|outcome|1516745010280|ba9745b1-28c4-4532-b1da-908aec4d0f41|dd602197-65e3-4e67-8130-d9d7feda41dc|\\|Doncaster\\||1/2|1|0|")
      .collect{case update:Outcome=>update}.get
    pub.sendNext(updateEvent)
    pub.sendNext(updateMarket)
    pub.sendNext(updateOutcome)
    pub.sendComplete()

    sub.expectNext(UpdateEventData(updateEvent))
    sub.expectNext(UpdateMarketData(updateMarket))
    sub.expectNext(UpdateOutcomeData(updateOutcome))
    sub.expectComplete()
  }

  override protected def afterAll():Unit = system.terminate()
}
