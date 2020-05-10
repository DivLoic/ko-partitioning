package fr.xebia.ldi.crocodile

import com.typesafe.config.ConfigFactory
import fr.xebia.ldi.crocodile.Configuration.CrocoConfig.{CrocoApp, CrocoTopic}
import fr.xebia.ldi.crocodile.Configuration.{CrocoConfig, _}
import fr.xebia.ldi.crocodile.schema.Account.{Free, Gold, Plus}
import fr.xebia.ldi.crocodile.schema.{Account, AccountId, Click, UserEvent}
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import org.apache.kafka.streams.test.TestRecord
import org.apache.kafka.streams.{TestInputTopic, TestOutputTopic, TopologyTestDriver}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, GivenWhenThen}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

/**
 * Created by loicmdivad.
 */
class StreamingAppSpec extends AnyFlatSpec
  with CrocoSerde
  with Matchers
  with BeforeAndAfterEach
  with BeforeAndAfterAll
  with GivenWhenThen {

  var testDriver: TopologyTestDriver = _

  var inputClickTopic: TestInputTopic[AccountId, Click] = _
  var inputAccountTopic: TestInputTopic[AccountId, Account] = _
  var outputResult: TestOutputTopic[AccountId, UserEvent] = _

  val testConfig: CrocoConfig = CrocoConfig(
    kafkaConfig = ConfigFactory.parseMap( Map(
        "application.id" -> "unit-test",
        "bootstrap.servers" -> "mock:9092"
      ).asJava
    ),
    application = CrocoApp(
      outputResult = CrocoTopic("TEST-OUT", 1, 1),
      inputClickTopic = CrocoTopic("TEST-IN-CLICKS", 1, 1),
      inputAccountTopic = CrocoTopic("TEST-IN-USERS", 1, 1)
    )
  )

  override def afterEach(): Unit = {
    testDriver.close()
  }

  override def beforeAll(): Unit = {
    clickSerde ::
    accountSerde ::
    accountIdSerde ::
    userEventSerde :: Nil foreach(
      _.configure(Map(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> "mock://notused") asJava, false)
    )

    accountIdSerde.configure(
      Map(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> "mock://notused") asJava, true
    )
  }

  override def beforeEach(): Unit = {
    val topology = StreamingTopology.buildTopology(testConfig)()

    testDriver = new TopologyTestDriver(topology, testConfig.kafkaConfig.toProps)

    inputClickTopic = testDriver.createInputTopic(
      testConfig.application.inputClickTopic.name,
      accountIdSerde.serializer(),
      clickSerde.serializer()
    )

    inputAccountTopic = testDriver.createInputTopic(
      testConfig.application.inputAccountTopic.name,
      accountIdSerde.serializer(),
      accountSerde.serializer()
    )

    outputResult = testDriver.createOutputTopic(
      testConfig.application.outputResult.name,
      accountIdSerde.deserializer(),
      userEventSerde.deserializer()
    )
  }


  "Application" should "join user events to account table" in {

    Given("...")
    inputAccountTopic.pipeRecordList(
      new TestRecord(AccountId("1"), Account("login1", Free)) ::
      new TestRecord(AccountId("2"), Account("login2", Gold)) ::
      new TestRecord(AccountId("3"), Account("login3", Plus)) :: Nil asJava
    )

    And("...")
    inputClickTopic.pipeRecordList(
      new TestRecord(AccountId("2"), Click("random-page-id")) :: Nil asJava
    )

    When("...")
    val resultNum: Long = outputResult.getQueueSize
    val result: Vector[TestRecord[AccountId, UserEvent]] = outputResult.readRecordsToList().asScala.toVector

    Then("...")

    resultNum shouldBe 1
    result.map(_.getKey) should contain only AccountId("2")
    result.map(_.getValue.login) should contain only "login2"
    result.map(_.getValue.plan) should contain only Some(Gold)
    result.map(_.getValue.pageId) should contain only "random-page-id"
  }
}
