package org.sunbird.job.task

import java.io.File
import java.util
import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.sunbird.job.mvcindexer.domain.Event
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.functions.MVCProcessorIndexer
import org.sunbird.job.util.{ElasticSearchUtil, FlinkUtil}


class MVCProcessorIndexerStreamTask(config: MVCProcessorIndexerConfig, kafkaConnector: FlinkKafkaConnector, esUtil: ElasticSearchUtil) {
  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
    implicit val mapTypeInfo: TypeInformation[util.Map[String, Any]] = TypeExtractor.getForClass(classOf[util.Map[String, Any]])
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

    val processStreamTask = env.addSource(kafkaConnector.kafkaJobRequestSource[Event](config.kafkaInputTopic)).name(config.eventConsumer)
      .uid(config.eventConsumer).setParallelism(config.kafkaConsumerParallelism)
      .rebalance
      .process(new MVCProcessorIndexer(config, esUtil))
      .name(config.mvcProcessorIndexerFunction)
      .uid(config.mvcProcessorIndexerFunction)
      .setParallelism(config.parallelism)

    processStreamTask.getSideOutput(config.failedOutputTag).addSink(kafkaConnector.kafkaStringSink(config.kafkaFailedTopic))
      .name(config.mvcFailedEventProducer).uid(config.mvcFailedEventProducer)

    env.execute(config.jobName)
  }
}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object MVCProcessorIndexerStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("mvc-processor-indexer.conf").withFallback(ConfigFactory.systemEnvironment()))
    val mvcProcessorIndexerConfig = new MVCProcessorIndexerConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(mvcProcessorIndexerConfig)
    val esUtil:ElasticSearchUtil = null
    val task = new MVCProcessorIndexerStreamTask(mvcProcessorIndexerConfig, kafkaUtil, esUtil)
    task.process()
  }
}

// $COVERAGE-ON$