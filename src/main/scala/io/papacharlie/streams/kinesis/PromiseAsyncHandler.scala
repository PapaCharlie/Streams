package io.papacharlie.streams.kinesis

import com.amazonaws.AmazonWebServiceRequest
import com.amazonaws.handlers.AsyncHandler
import com.twitter.util.Promise

class PromiseAsyncHandler[A <: AmazonWebServiceRequest, B](
  promise: Promise[B]
) extends AsyncHandler[A, B] {
  def onError(exception: Exception) = promise.setException(exception)
  def onSuccess(request: A, result: B) = promise.setValue(result)
}
