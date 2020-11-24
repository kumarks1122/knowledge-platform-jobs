package org.sunbird.job.functions

//import java.util.UUID

import java.{lang, util}

import com.google.gson.Gson
import org.apache.commons.lang3.StringUtils
import org.json4s.jackson.JsonMethods.parse
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.json4s.DefaultFormats
import org.json4s.jackson.Json
import org.slf4j.LoggerFactory
//import org.sunbird.job.cache.{DataCache, RedisConnect}
import org.sunbird.job.service.VideoStreamService
import org.sunbird.job.task.VideoStreamGeneratorConfig
import org.sunbird.job.{Metrics, WindowBaseProcessFunction}

import scala.collection.JavaConverters._


class VideoStreamGenerator(config: VideoStreamGeneratorConfig)
                          (implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]])
                          extends WindowBaseProcessFunction[util.Map[String, AnyRef], String, String](config) {


    implicit lazy val videoStreamConfig: VideoStreamGeneratorConfig = config
    private[this] lazy val logger = LoggerFactory.getLogger(classOf[VideoStreamGenerator])
    private var videoStreamService:VideoStreamService = _

    override def metricsList(): List[String] = {
        List(config.successEventCount, config.failedEventCount, config.batchEnrolmentUpdateEventCount, config.dbUpdateCount, config.dbReadCount, config.cacheHitCount, config.skipEventsCount, config.cacheMissCount)
    }

    override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        videoStreamService = new VideoStreamService();
    }

    override def close(): Unit = {
        videoStreamService.closeConnection()
        super.close()
    }

    override def process(key: String, context: ProcessWindowFunction[util.Map[String, AnyRef], String, String, TimeWindow]#Context, events: lang.Iterable[util.Map[String, AnyRef]], metrics: Metrics): Unit = {
        val gson = new Gson()
        events.asScala.foreach { event =>
            logger.info("Event eid:: "+ event.get("eid"))
            val eventData = parse(gson.toJson(event)).values.asInstanceOf[Map[String, AnyRef]]
            if(isValidEvent(eventData("edata").asInstanceOf[Map[String, AnyRef]])) {
                logger.info("Event eid:: valid event")
                videoStreamService.submitJobRequest(eventData)
            }
        }
    }

    private def isValidEvent(eData: Map[String, AnyRef]): Boolean = {
        val artifactUrl = eData.getOrElse("artifactUrl", "").asInstanceOf[String]
        val mimeType = eData.getOrElse("mimeType", "").asInstanceOf[String]
        val identifier = eData.getOrElse("identifier", "").asInstanceOf[String]

        StringUtils.isNotBlank(artifactUrl) &&
          StringUtils.isNotBlank(mimeType) &&
          (
            StringUtils.equalsIgnoreCase(mimeType, "video/mp4") ||
              StringUtils.equalsIgnoreCase(mimeType, "video/webm")
            ) &&
          StringUtils.isNotBlank(identifier)
    }
}
