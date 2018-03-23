package sbgfeedme.consumer

import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}

/** Graph stage to aggregate the create child messages (markets, outcomes) of an event. Update messages are not aggregated. */
class AggregationStage extends GraphStage[FlowShape[Message,Aggregate]] {
  val in:Inlet[Message] = Inlet[Message]("AggregationStage.in")
  val out:Outlet[Aggregate] = Outlet[Aggregate]("AggregationStage.out")
  val shape:FlowShape[Message,Aggregate] = FlowShape.of(in, out)

  def createLogic(inheritedAttributes:Attributes):GraphStageLogic = new GraphStageLogic(shape) with StageLogging {
    var currentCreateEventData = Option.empty[CreateEventData]
    var stillToPush = Option.empty[Aggregate]
    setHandler(in, new InHandler {
      def onPush():Unit = {
        log.debug("onPush()")
        val message = grab(in)
        message.operation match {
          case Create =>
            message match {
              case event:Event =>
                val createEventData = CreateEventData(event, List.empty[CreateMarketData])
                if (currentCreateEventData.isDefined) {
                  push(out, currentCreateEventData.get)
                  currentCreateEventData = Some(createEventData)
                } else {
                  currentCreateEventData = Some(createEventData)
                  pull(in)
                }
              case market:Market =>
                if (currentCreateEventData.isDefined) {
                  currentCreateEventData = Some(currentCreateEventData.get.addMarketData(CreateMarketData(market, List.empty[Outcome])))
                }
                pull(in)
              case outcome:Outcome =>
                if (currentCreateEventData.isDefined) {
                  currentCreateEventData = currentCreateEventData.get.addOutcome(outcome)
                }
                pull(in)
            }
          case Update =>
            val update = message match {
              case event:Event => UpdateEventData(event)
              case market:Market => UpdateMarketData(market)
              case outcome:Outcome => UpdateOutcomeData(outcome)
            }
            if (currentCreateEventData.isDefined) {
              push(out, currentCreateEventData.get)
              currentCreateEventData = None
              stillToPush = Some(update)
            } else {
              push(out, update)
            }
        }
      }
      override def onUpstreamFinish():Unit = {
        if (stillToPush.isDefined) {
          push(out, stillToPush.get)
          stillToPush = None
        }
        super.onUpstreamFinish()
      }
    })
    setHandler(out, new OutHandler {
      def onPull():Unit = {
        log.debug("onPull()")
        if (stillToPush.isDefined) {
          push(out, stillToPush.get)
          stillToPush = None
        } else {
          pull(in)
        }
      }
    })
  }
}
