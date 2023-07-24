package com.example

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.{
  AttributeDefinition,
  CreateTableRequest,
  DescribeTableRequest,
  KeySchemaElement,
  KeyType,
  ProvisionedThroughput,
  ResourceInUseException,
  ScalarAttributeType,
  StreamSpecification,
  StreamViewType
}

import scala.jdk.CollectionConverters.SeqHasAsJava
object DynamoDBHelper {
  private final val TABLE_NAME = "KCL-Demo"
}

class DynamoDBHelper(client: AmazonDynamoDB) {
  import DynamoDBHelper._

  def setUpTable(): String = {
    val attributeDefinitions = Vector(
      new AttributeDefinition("Id", ScalarAttributeType.N)
    )
    val keySchema = Vector(
      new KeySchemaElement("Id", KeyType.HASH)
    )

    val provisionedThroughput =
      new ProvisionedThroughput()
        .withReadCapacityUnits(2L)
        .withWriteCapacityUnits(2L)
    val streamSpecification = new StreamSpecification()
      .withStreamEnabled(true)
      .withStreamViewType(StreamViewType.NEW_IMAGE)

    val createTableRequest = new CreateTableRequest()
      .withTableName(TABLE_NAME)
      .withAttributeDefinitions(attributeDefinitions.asJava)
      .withKeySchema(keySchema.asJava)
      .withProvisionedThroughput(provisionedThroughput)
      .withStreamSpecification(streamSpecification)
    try {
      println("Creating table " + TABLE_NAME)
      val createTableResult = client
        .createTable(createTableRequest)
      createTableResult.getTableDescription.getLatestStreamArn
    } catch {
      case _: ResourceInUseException =>
        println(s"Table ${TABLE_NAME} already exists.")
        val describeTableResult = client
          .describeTable(new DescribeTableRequest(TABLE_NAME))
        describeTableResult.getTable.getLatestStreamArn
    }
  }
}
