use actix_web::{
    dev::ServiceRequest,
    error::{ErrorForbidden, ErrorInternalServerError},
    Error, HttpResponse,
};
use actix_web_httpauth::extractors::bearer::BearerAuth;

/// Auth validator for all endpoints using HTTP bearer token.
pub async fn validator(
    req: ServiceRequest,
    credentials: BearerAuth,
) -> Result<ServiceRequest, (Error, ServiceRequest)> {
    let Some(expected_token) = req.app_data::<actix_web::web::Data<String>>() else {
        return Err((ErrorInternalServerError("auth token not configured"), req));
    };

    if credentials.token() != expected_token.get_ref() {
        return Err((ErrorForbidden("forbidden"), req));
    }

    Ok(req)
}

/// Readiness response used by health endpoints.
pub fn ok() -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({ "status": "ok" }))
}
