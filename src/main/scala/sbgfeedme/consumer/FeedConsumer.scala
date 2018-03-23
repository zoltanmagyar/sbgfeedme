package sbgfeedme.consumer

import akka.Done
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.{Flow, Framing, Tcp}
import akka.util.ByteString
import com.mongodb.MongoCommandException
import org.json4s._
import org.json4s.native.Serialization.write
import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.model.Updates.set
import org.mongodb.scala.model.{Filters, UpdateOptions}
import org.mongodb.scala.{Completed, MongoClient, MongoCollection}
import sbgfeedme.consumer.Constants._

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try

/** Connects to the upstream feed and starts consuming messages */
object FeedConsumer extends App {
  implicit val system:ActorSystem = ActorSystem()
  implicit val materializer:ActorMaterializer = ActorMaterializer()
  implicit val formats = DefaultFormats+new OperationSerializer+FieldSerializer[CreateEventData with Aggregate]()

  // Register a shutdown hook to close system resources when app is stopped
  sys.addShutdownHook {
    system.terminate()
    mongoClient.foreach(_.close())
  }

  // Parse each raw message as produced by the upstream feed into a Message. Discard invalid messages
  val parse = Flow[ByteString]
    .map(byteString=> Message(byteString.utf8String))
    .collect { case Some(m) => m }

  // Aggregate all create operations for each root level event data into a single record. Push update operations downstream unchanged
  val aggregate = new AggregationStage

  // Persist the aggregated records into the default Mongo collection or perform the required updates
  def persist(collection:MongoCollection[Document]) = Flow[Aggregate]
    .mapAsync(1) {
      case createEvent:CreateEventData =>
        collection.insertOne(Document(write(createEvent))).toFuture().map(_=> Done)
      case updateEvent:UpdateEventData =>
        collection.updateOne(
          Filters.eq(MongoDocumentIdFieldName, updateEvent.event.eventId),
          set("event", Document(write(updateEvent.event)))).toFuture().map(_=> Done)
      case updateMarket:UpdateMarketData =>
        collection.updateOne(
          Filters.and(
            Filters.eq(MongoDocumentIdFieldName, updateMarket.market.eventId),
            Filters.eq("markets.market.marketId", updateMarket.market.marketId)),
          set("markets.$.market", Document(write(updateMarket.market)))).toFuture().map(_=> Done)
      case updateOutcome:UpdateOutcomeData =>
        val arrayFilters = List(
          Filters.eq("i.market.marketId", updateOutcome.outcome.marketId),
          Filters.eq("j.outcomeId", updateOutcome.outcome.outcomeId))
        collection.updateOne(
          Filters.eq("markets.market.marketId", updateOutcome.outcome.marketId),
          set("markets.$[i].outcomes.$[j]", Document(write(updateOutcome.outcome))),
          UpdateOptions().arrayFilters(arrayFilters.asJava)).toFuture().map(_=> Done)
    }

  // Build a flow from the above stages
  def createMessageFlow(collection:MongoCollection[Document]) = Flow[ByteString]
    .via(Framing.delimiter(ByteString("\n"), maximumFrameLength=256, allowTruncation=true))
    .via(parse)
    .via(aggregate)
    .via(persist(collection))
    .map(_=> ByteString.empty)

  val mongoClient = Future(MongoClient(s"mongodb://$MongoHost:$MongoPort"))
  val flowResult = for {
    client <- mongoClient
    database = client.getDatabase(MongoDatabaseName)
    _ <- database.createCollection(MongoCollectionName).toFuture().recover {
      case e:MongoCommandException if e.getErrorCode == MongoNamespaceExistsErrorCode => Completed
    }
    collection = database.getCollection[Document](MongoCollectionName)
    runnable = Tcp().outgoingConnection(FeedProviderHost, FeedProviderPort).join(createMessageFlow(collection))
    _ <- runnable.run()
  } yield {
    Done
  }
  Try(Await.result(flowResult, Timeout)).getOrElse(sys.exit(130))
}
/** Set of useful constants */
object Constants {
  /** Feed provider host */
  val FeedProviderHost = "provider"
  /** Feed provider port */
  val FeedProviderPort = 8282
  /** Mongo host */
  val MongoHost = "mongo"
  /** Mongo port */
  val MongoPort = 27017
  /** Default Mongo database name */
  val MongoDatabaseName = "feedme"
  /** Default Mongo collection name */
  val MongoCollectionName = "events"
  /** Default Mongo document ID field name */
  val MongoDocumentIdFieldName = "_id"
  /** Error code for NamespaceExists error */
  val MongoNamespaceExistsErrorCode = 48
  /** Default Future timeout */
  val Timeout:Duration = 10.seconds
}
