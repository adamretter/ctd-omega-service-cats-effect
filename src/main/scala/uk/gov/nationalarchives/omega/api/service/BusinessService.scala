package uk.gov.nationalarchives.omega.api.service

import cats.data.ValidatedNec

trait BusinessServiceRequest
trait BusinessServiceResponse

trait BusinessServiceError {
//  def reference: String // TODO(AR) this comes later and is used for the user to contact the helpdesk
  def code: String
  def message: String
}

trait BusinessService[T <: BusinessServiceRequest, U <: BusinessServiceResponse, E <: BusinessServiceError] {
  def process(request: T): Either[BusinessServiceError, U]
}

trait RequestValidationError {
  def errorMessage: String
}

trait RequestValidation[T <: BusinessServiceRequest] {

  type ValidationResult = ValidatedNec[RequestValidationError, T]

  def validateRequest(request: T): ValidationResult //Validated[RequestValidationError, T]
}
