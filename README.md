# FeedMe Tech Test Solution using Akka Streams

Solution to a Sky B&G [coding test](https://github.com/skybet/feedme-tech-test/).

## Design

The consumer app has been implemented in Scala using the Akka Streams library.

The stream processing pipeline implemented has the following stages:
* Parse stage: Parses each incoming message into the appropriate object model as defined by the schema. Messages that don't conform to this schema are discarded.
* Aggregation stage: Aggregates all children for a `create` event message. Outputs a single record for an event including all its markets and their corresponding outcomes. `update` messages are not aggregated and are pushed downstream unchanged.
* Persistence stage using MongoDB: Persists the records into a MongoDB collection. For each event a new document is created containing the event data and the child markets and outcomes. Updates are performed at different levels of the document hierarchy.

## Instructions to run

The project can be built using a recent version of `sbt`.

To build it including unit tests, run `sbt test` in the project directory.

To build a docker image, run `sbt docker`. After the image has built, run `docker-compose up` to start consuming the live feed.