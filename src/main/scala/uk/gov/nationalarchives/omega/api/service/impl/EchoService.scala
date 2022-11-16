package uk.gov.nationalarchives.omega.api.service.impl

import cats.data.{NonEmptyChain, Validated}
import uk.gov.nationalarchives.omega.api.service.{BusinessService, BusinessServiceError, BusinessServiceRequest, BusinessServiceResponse, RequestValidation, RequestValidationError}

case class EchoRequest(text: String) extends BusinessServiceRequest
case class EchoResponse(text: String) extends BusinessServiceResponse

sealed trait EchoServiceError extends BusinessServiceError
case class EchoExplicitError(message: String) extends EchoServiceError {
  override val code = "ERR-ECHO-001"
}

class EchoService extends BusinessService[EchoRequest, EchoResponse, EchoServiceError]
    with RequestValidation[EchoRequest] {

  override def validateRequest(request: EchoRequest): ValidationResult = {
    Validated.cond(request.text.nonEmpty, request, NonEmptyChain.one(TextIsNonEmptyCharacters))
  }

  override def process(request: EchoRequest): Either[EchoServiceError, EchoResponse] = {
    if (request.text.contains("ERROR")) {
      Left(EchoExplicitError(s"Explicit error: ${request.text}"))
    } else {
      Right(EchoResponse(request.text))
    }
  }
}

case object TextIsNonEmptyCharacters extends RequestValidationError {
  def errorMessage: String = "Echo Text cannot be empty."
}


//sealed trait EchoRequestValidator {
//  type ValidationResult[A] = ValidatedNec[RequestValidation, A]
//
//  private def validateText(text: String): ValidationResult[String] =
//    if (text.nonEmpty) text.validNec else TextIsNonEmptyCharacters.invalidNec
//
//  def validateRequest(request: EchoRequest): ValidationResult[EchoRequest] = {
//    validateText(request.text).map(EchoRequest)
//  }
//}