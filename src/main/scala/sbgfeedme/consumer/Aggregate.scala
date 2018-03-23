package sbgfeedme.consumer

/** Represents a message that is output from the aggregation stage */
sealed trait Aggregate
/** Holds all the data for an event including all of its markets */
case class CreateEventData(event:Event, markets:List[CreateMarketData]) extends Aggregate {
  /** Unique Event ID */
  val _id:String = event.eventId
  def addMarketData(createMarketData:CreateMarketData):CreateEventData = this.copy(markets=createMarketData::this.markets)
  def addOutcome(outcome:Outcome):Option[CreateEventData] = markets.headOption.map {market=>
    val marketWithNewOutcome = market.copy(outcomes=outcome::market.outcomes)
    this.copy(markets=marketWithNewOutcome::this.markets.tail)
  }
}
/** Holds all the data for a market including all of its outcomes */
case class CreateMarketData(market:Market, outcomes:List[Outcome])
/** Holds all the data for an event update message */
case class UpdateEventData(event:Event) extends Aggregate
/** Holds all the data for a market update message */
case class UpdateMarketData(market:Market) extends Aggregate
/** Holds all the data for an outcome update message */
case class UpdateOutcomeData(outcome:Outcome) extends Aggregate
