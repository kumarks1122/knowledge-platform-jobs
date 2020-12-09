package org.sunbird.job.util


trait Result {

  def getSubmitJobResult(response: MediaResponse): Map[String, AnyRef]

  def getJobResult(response: MediaResponse): Map[String, AnyRef]

  def getCancelJobResult(response: MediaResponse): Map[String, AnyRef]

  def getListJobResult(response: MediaResponse): Map[String, AnyRef]
}
