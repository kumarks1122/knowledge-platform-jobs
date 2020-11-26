package org.sunbird.job.functions

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.{ReducingState, ReducingStateDescriptor}
import org.apache.flink.api.common.typeutils.base.LongSerializer
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory
import org.sunbird.job.service.VideoStreamService
import org.sunbird.job.task.VideoStreamGeneratorConfig

class VideoStreamGeneratorTrigger[W <: TimeWindow](maxCount: Long, timeCharacteristic: TimeCharacteristic)(implicit config: VideoStreamGeneratorConfig) extends Trigger[Object, W] {

  private val countState: ReducingStateDescriptor[java.lang.Long] = new ReducingStateDescriptor[java.lang.Long]("count", new Sum(), LongSerializer.INSTANCE)
  private[this] lazy val logger = LoggerFactory.getLogger(classOf[VideoStreamGeneratorTrigger[W]])
  lazy val videoStreamService:VideoStreamService = new VideoStreamService();

  override def onElement(element: Object, timestamp: Long, window: W, ctx: TriggerContext): TriggerResult = {
    val count: ReducingState[java.lang.Long] = ctx.getPartitionedState(countState)
    logger.info("Trigger Event onElement")
    count.add(1L)
    if (count.get >= maxCount || timestamp >= window.getEnd) {
      logger.info("inside onElement::" + element.toString())
      count.clear()
      TriggerResult.FIRE_AND_PURGE
    } else {
      TriggerResult.CONTINUE
    }
  }

  override def onProcessingTime (time: Long, window: W, ctx: TriggerContext): TriggerResult = {
    logger.info("Trigger Event onProcessingTime")
    if (timeCharacteristic == TimeCharacteristic.EventTime) TriggerResult.CONTINUE
    else {
      if (time >= window.getEnd) TriggerResult.CONTINUE else {
        TriggerResult.PURGE
      }
    }
  }

  override def onEventTime (time: Long, window: W, ctx: TriggerContext): TriggerResult = {
    logger.info("Trigger Event onEventTime")
    if (timeCharacteristic == TimeCharacteristic.ProcessingTime) TriggerResult.CONTINUE else {
      if (time >= window.getEnd) TriggerResult.CONTINUE else {
        logger.info("inside onEventTime::"+time.toOctalString)
        videoStreamService.processJobRequest()
        videoStreamService.closeConnection()
        TriggerResult.PURGE
      }
    }
  }

  override def clear (window: W, ctx: TriggerContext): Unit = {
    logger.info("inside clear method::")
    ctx.getPartitionedState (countState).clear
  }

  class Sum extends ReduceFunction[java.lang.Long] {
    def reduce(value1: java.lang.Long, value2: java.lang.Long): java.lang.Long = value1 + value2
  }
}
