package org.sunbird.job.task

import java.io.File
import java.util

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.functions.{VideoStreamGenerator, VideoStreamGeneratorTrigger}
import org.sunbird.job.util.FlinkUtil


class VideoStreamGeneratorStreamTask(config: VideoStreamGeneratorConfig, kafkaConnector: FlinkKafkaConnector) {
  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
//    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

    env.addSource(kafkaConnector.kafkaMapSource(config.kafkaInputTopic), config.videoStreamConsumer)
      .uid(config.videoStreamConsumer).setParallelism(config.kafkaConsumerParallelism)
      .rebalance()
      .keyBy(x => x.get("partition").toString)
      .timeWindow(Time.seconds(3))
      .trigger(new VideoStreamGeneratorTrigger[TimeWindow](1, env.getStreamTimeCharacteristic)(config: VideoStreamGeneratorConfig))
      .process(new VideoStreamGenerator(config))
      .name("video-stream-generator")
      .setParallelism(config.parallelism)

    env.execute(config.jobName)
  }
}


// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object VideoStreamGeneratorStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("video-stream-generator.conf").withFallback(ConfigFactory.systemEnvironment()))
    val videoStreamConfig = new VideoStreamGeneratorConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(videoStreamConfig)
    val task = new VideoStreamGeneratorStreamTask(videoStreamConfig, kafkaUtil)
    task.process()
  }
}

// $COVERAGE-ON$
