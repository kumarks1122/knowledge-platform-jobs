package org.sunbird.job.service

import java.util.UUID

import com.datastax.driver.core.querybuilder.{QueryBuilder, Select}
import org.joda.time.DateTime
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Json
import org.json4s.DefaultFormats
import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.functions.VideoStreamGenerator
import org.sunbird.job.service.impl.MediaServiceFactory
import org.sunbird.job.task.VideoStreamGeneratorConfig
import org.sunbird.job.util.{CassandraUtil, HttpRestUtil, JobRequest, MediaRequest, MediaResponse, StreamingStage}

import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap

class VideoStreamService(implicit config: VideoStreamGeneratorConfig) {
  implicit val formats: DefaultFormats.type = DefaultFormats
  private[this] lazy val logger = LoggerFactory.getLogger(classOf[VideoStreamGenerator])
  private lazy val mediaService = MediaServiceFactory.getMediaService()
  private lazy val dbKeyspace:String = config.dbKeyspace
  private lazy val dbTable:String = config.dbTable
  lazy val cassandraUtil:CassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
  private lazy val clientKey:String = "SYSTEM_LP"
  private lazy val SUBMITTED:String = "SUBMITTED"
  private lazy val VIDEO_STREAMING:String = "VIDEO_STREAMING"


  def submitJobRequest(eventData: Map[String, AnyRef]): Unit = {
    val stageName = "STREAMING_JOB_SUBMISSION";
    val eData:Map[String, AnyRef] = eventData.get("edata").get.asInstanceOf[Map[String, AnyRef]]
    val jobSubmitted = DateTime.now()
    val requestId = UUID.randomUUID().toString
    val jobRequest = JobRequest(clientKey, requestId, None, SUBMITTED, Json(DefaultFormats).write(eData), 0, Option(jobSubmitted),
                                Option(eData.getOrElse("artifactUrl", "").asInstanceOf[String]), None, None, None, None, None,
                                None, None, None, None, None, None, None, Option(stageName), Option(SUBMITTED), Option(VIDEO_STREAMING))

    saveJobRequest(jobRequest)
    submitStreamJob(jobRequest)
  }

  def processJobRequest(): Unit = {
    updateProcessingRequest()
    resubmitFailedJob()
  }

  def updateProcessingRequest(): Unit = {
    val processingJobRequests = readFromDB(Map("status" -> "PROCESSING"))
    val stageName = "STREAMING_JOB_COMPLETE"

    processingJobRequests.map{ jobRequest =>
      val mediaResponse:MediaResponse = mediaService.getJob(jobRequest.job_id.get)
      logger.info("Get job details while saving.", Option(mediaResponse))
      val iteration = jobRequest.iteration

      if(mediaResponse.responseCode.contentEquals("OK")) {
        val jobStatus = mediaResponse.result.getOrElse("job", Map()).asInstanceOf[Map[String, AnyRef]].getOrElse("status","").asInstanceOf[String];

        if(jobStatus.equalsIgnoreCase("FINISHED")) {
          val streamingUrl = mediaService.getStreamingPaths(jobRequest.job_id.get).result.getOrElse("streamUrl","").asInstanceOf[String]
          val requestData = parse(jobRequest.request_data).values.asInstanceOf[Map[String, AnyRef]]
          val contentId = requestData.getOrElse("content_id", "").asInstanceOf[String]
          val channel = requestData.getOrElse("channel", "").asInstanceOf[String]

          if(updatePreviewUrl(contentId, streamingUrl, channel)) {
            StreamingStage(jobRequest.request_id, jobRequest.client_key, jobRequest.job_id.get, stageName, jobStatus, "FINISHED", iteration + 1);
          } else {
            null
          }
        } else if(jobStatus.equalsIgnoreCase("ERROR")){
          StreamingStage(jobRequest.request_id, jobRequest.client_key, jobRequest.job_id.get, stageName, jobStatus, "FAILED", iteration + 1);
        } else {
          null
        }
      } else {
        val errorMsg = mediaResponse.result.toString
        StreamingStage(jobRequest.request_id, jobRequest.client_key, null, stageName, "FAILED", "FAILED", iteration + 1, errorMsg);
      }
    }.filter(x =>  x != null).map{ streamStage:StreamingStage =>
      updateJobRequestStage(streamStage)
    }
  }

  def resubmitFailedJob(): Unit = {
    val failedJobRequests = readFromDB(Map("status" -> "FAILED", "iteration" -> Map("type"-> "lte", "value" -> 10))).toArray

    failedJobRequests.foreach { jobRequest =>
      submitStreamJob(jobRequest)
    }
  }

  def submitStreamJob(jobRequest: JobRequest): Unit = {

    val requestData = parse(jobRequest.request_data).values.asInstanceOf[Map[String, AnyRef]]
    val mediaRequest = MediaRequest(UUID.randomUUID().toString, null, requestData)
    val response:MediaResponse = mediaService.submitJob(mediaRequest)
    val stageName = "STREAMING_JOB_SUBMISSION";
    var streamStage:Option[StreamingStage] = None;

    if (response.responseCode.equals("OK")) {
      val jobId = response.result.getOrElse("job", Map()).asInstanceOf[Map[String, AnyRef]].getOrElse("id","").asInstanceOf[String];
      val jobStatus = response.result.getOrElse("job", Map()).asInstanceOf[Map[String, AnyRef]].getOrElse("status","").asInstanceOf[String];
      streamStage = Option(StreamingStage(jobRequest.request_id, jobRequest.client_key, jobId, stageName, jobStatus, "PROCESSING", jobRequest.iteration + 1))
    } else {
      val errorMsg = response.result.toString

      streamStage = Option(StreamingStage(jobRequest.request_id, jobRequest.client_key, null, stageName, "FAILED", "FAILED", jobRequest.iteration + 1, errorMsg));
    }

    updateJobRequestStage(streamStage.get);
  }

  private def updatePreviewUrl(contentId: String, streamingUrl: String, channel: String): Boolean = {
    if(!streamingUrl.isEmpty && !contentId.isEmpty) {
      val requestBody = "{\"request\": {\"content\": {\"streamingUrl\":\""+ streamingUrl +"\"}}}"
      val url = config.lpURL + config.contentV3Update + contentId
      val headers = HashMap[String, String]("X-Channel-Id" -> channel)
      val response:MediaResponse = HttpRestUtil.put(url, headers, requestBody)

      if(response.responseCode.contentEquals("OK")){
        true;
      } else{
        logger.error("Error while updating previewUrl for content : " + contentId , Option(response))
        false
      }
    }else{
      false
    }
  }

  def readFromDB(columns: Map[String, AnyRef]): List[JobRequest] = {
    val selectWhere: Select.Where = QueryBuilder.select().all()
      .from(dbKeyspace, dbTable)
      .allowFiltering()
      .where()

    columns.map(col => {
      col._2 match {
        case value: List[Any] =>
          selectWhere.and(QueryBuilder.in(col._1, value.asJava))
        case value: Map[String, AnyRef] =>
          if (value.get("type").get == "lte") {
            selectWhere.and(QueryBuilder.lte(col._1, value.get("value").get))
          } else {
            selectWhere.and(QueryBuilder.gte(col._1, value.get("value").get))
          }
        case _ =>
          selectWhere.and(QueryBuilder.eq(col._1, col._2))
      }
    })

    selectWhere.orderBy(QueryBuilder.asc("dt_job_submitted"))

    val result = cassandraUtil.find(selectWhere.toString).asScala.toList.map { jr =>
      JobRequest(jr.getString("client_key"), jr.getString("request_id"), Option(jr.getString("job_id")), jr.getString("status"), jr.getString("request_data"), jr.getInt("iteration"), stage=Option(jr.getString("stage")), stage_status=Option(jr.getString("stage_status")),job_name=Option(jr.getString("job_name")))
    }
    result
  }

  def saveJobRequest(jobRequest: JobRequest): Boolean = {
    val query = QueryBuilder.insertInto(dbKeyspace, dbTable)
      .value("client_key", jobRequest.client_key)
      .value("request_id", jobRequest.request_id)
      .value("job_id", jobRequest.job_id.getOrElse(""))
      .value("status", jobRequest.status)
      .value("request_data", jobRequest.request_data)
      .value("iteration", jobRequest.iteration)
      .value("dt_job_submitted", setDateColumn(jobRequest.dt_job_submitted).get)
      .value("location", jobRequest.location.get)
//      .value("dt_file_created", setDateColumn(jobRequest.dt_file_created).get)
//      .value("dt_first_event", setDateColumn(jobRequest.dt_first_event).get)
//      .value("dt_last_event", setDateColumn(jobRequest.dt_last_event).get)
//      .value("dt_expiration", setDateColumn(jobRequest.dt_expiration).get)
//      .value("dt_job_processing", setDateColumn(jobRequest.dt_job_processing).get)
//      .value("dt_job_completed", setDateColumn(jobRequest.dt_job_completed).get)
//      .value("input_events", jobRequest.input_events.getOrElse(0))
//      .value("output_events", jobRequest.output_events.getOrElse(0))
//      .value("file_size", jobRequest.file_size.getOrElse(0L))
//      .value("latency", jobRequest.latency.getOrElse(0))
//      .value("execution_time", jobRequest.execution_time.getOrElse(0L))
//      .value("err_message", jobRequest.err_message.getOrElse(""))
      .value("stage", jobRequest.stage.get)
      .value("stage_status", jobRequest.stage_status.get)
      .value("job_name", jobRequest.job_name.get)

    val result = cassandraUtil.session.execute(query)
    result.wasApplied()
  }

  def updateJobRequestStage(streamStage: StreamingStage): Boolean = {
    val query = QueryBuilder.update(dbKeyspace, dbTable)
      .`with`(QueryBuilder.set("job_id", streamStage.job_id))
      .and(QueryBuilder.set("stage", streamStage.stage))
      .and(QueryBuilder.set("stage_status", streamStage.stage_status))
      .and(QueryBuilder.set("status", streamStage.status))
      .and(QueryBuilder.set("iteration", streamStage.iteration))
      .and(QueryBuilder.set("err_message", streamStage.err_message))
      .where(QueryBuilder.eq("request_id", streamStage.request_id))
      .and(QueryBuilder.eq("client_key", streamStage.client_key))

    cassandraUtil.upsert(query.toString)
  }

  def setDateColumn(date: Option[DateTime]): Option[Long] = {
    val timestamp = date.orNull
    if (null == timestamp) None else Option(timestamp.getMillis)
  }

  def closeConnection(): Unit = {
    cassandraUtil.close()
  }
}
