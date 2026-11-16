use axum::{Json, extract::{State, Path}, http::{HeaderMap, StatusCode}};
use serde::{Deserialize, Serialize};
use crate::api::router::AppState;
use crate::domain::models::CardToken;
use uuid::Uuid;
use tracing::error;

#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

#[derive(Deserialize)]
pub struct AuthorizeRequest {
    pub order_id: String,
    pub customer_id: String,
    pub amount_cents: i64,
    pub card_number: String,
    pub cvv: String,
    pub card_last_four: String,
    pub card_expiry_month: u8,
    pub card_expiry_year: u16,
    pub idempotency_key: Option<String>,
}

#[derive(Serialize)]
pub struct AuthorizeResponse {
    pub payment_id: String,
    pub state: String,
}

#[derive(Serialize)]
pub struct PaymentSummary {
    pub payment_id: String,
    pub order_id: String,
    pub customer_id: String,
    pub amount_cents: i64,
    pub currency: String,
    pub state: String,
    pub bank_reference: Option<String>,
    pub last_four: Option<String>,
}

pub async fn authorize(
    State(state): State<AppState>,
    Json(req): Json<AuthorizeRequest>,
) -> Result<Json<AuthorizeResponse>, (StatusCode, Json<ErrorResponse>)> {
    let card_token = CardToken {
        card_number: req.card_number,
        cvv: req.cvv,
        last_four: req.card_last_four,
        expiry_month: req.card_expiry_month,
        expiry_year: req.card_expiry_year,
    };

    let payment = state.service.authorize(
        req.order_id,
        req.customer_id,
        req.amount_cents,
        card_token,
        req.idempotency_key,
    ).await.map_err(|e| {
        error!("authorize failed: {e}");
        (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: e.to_string() }))
    })?;

    Ok(Json(AuthorizeResponse {
        payment_id: payment.id.to_string(),
        state: format!("{:?}", payment.state),
    }))
}

fn idempotency_key_from_headers(headers: &HeaderMap) -> Option<String> {
    headers
        .get("Idempotency-Key")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}

pub async fn capture(
    State(state): State<AppState>,
    Path(payment_id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let idempotency_key = idempotency_key_from_headers(&headers);
    state.service.capture(payment_id, idempotency_key).await
        .map(|_| StatusCode::OK)
        .map_err(|e| {
            error!("capture failed for payment {}: {e}", payment_id);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: e.to_string() }))
        })
}

pub async fn void_payment(
    State(state): State<AppState>,
    Path(payment_id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let idempotency_key = idempotency_key_from_headers(&headers);
    state.service.void(payment_id, idempotency_key).await
        .map(|_| StatusCode::OK)
        .map_err(|e| {
            error!("void failed for payment {}: {e}", payment_id);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: e.to_string() }))
        })
}

pub async fn refund(
    State(state): State<AppState>,
    Path(payment_id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let idempotency_key = idempotency_key_from_headers(&headers);
    state.service.refund(payment_id, idempotency_key).await
        .map(|_| StatusCode::OK)
        .map_err(|e| {
            error!("refund failed for payment {}: {e}", payment_id);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: e.to_string() }))
        })
}

pub async fn get_payment(
    State(state): State<AppState>,
    Path(payment_id): Path<Uuid>,
) -> Result<Json<PaymentSummary>, (StatusCode, Json<ErrorResponse>)> {
    let payment = state.service.get(payment_id).await
        .map_err(|e| {
            error!("get_payment failed for {}: {e}", payment_id);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: e.to_string() }))
        })?
        .ok_or((StatusCode::NOT_FOUND, Json(ErrorResponse { error: "not found".into() })))?;

    Ok(Json(PaymentSummary {
        payment_id: payment.id.to_string(),
        order_id: payment.order_id,
        customer_id: payment.customer_id,
        amount_cents: payment.amount_cents,
        currency: payment.currency,
        state: format!("{:?}", payment.state),
        bank_reference: payment.bank_reference,
        last_four: payment.last_four,
    }))
}

pub async fn get_by_order(
    State(state): State<AppState>,
    Path(order_id): Path<String>,
) -> Result<Json<PaymentSummary>, (StatusCode, Json<ErrorResponse>)> {
    let payment = state.service.get_by_order(&order_id).await
        .map_err(|e| {
            error!("get_by_order failed for {}: {e}", order_id);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: e.to_string() }))
        })?
        .ok_or((StatusCode::NOT_FOUND, Json(ErrorResponse { error: "not found".into() })))?;

    Ok(Json(PaymentSummary {
        payment_id: payment.id.to_string(),
        order_id: payment.order_id,
        customer_id: payment.customer_id,
        amount_cents: payment.amount_cents,
        currency: payment.currency,
        state: format!("{:?}", payment.state),
        bank_reference: payment.bank_reference,
        last_four: payment.last_four,
    }))
}

pub async fn list_by_customer(
    State(state): State<AppState>,
    Path(customer_id): Path<String>,
) -> Result<Json<Vec<PaymentSummary>>, (StatusCode, Json<ErrorResponse>)> {
    let payments = state.service.list_by_customer(&customer_id).await
        .map_err(|e| {
            error!("list_by_customer failed for {}: {e}", customer_id);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: e.to_string() }))
        })?;

    Ok(Json(payments.into_iter().map(|p| PaymentSummary {
        payment_id: p.id.to_string(),
        order_id: p.order_id,
        customer_id: p.customer_id,
        amount_cents: p.amount_cents,
        currency: p.currency,
        state: format!("{:?}", p.state),
        bank_reference: p.bank_reference,
        last_four: p.last_four,
    }).collect()))
}

/// Serve the OpenAPI JSON specification.
pub async fn openapi_spec() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "openapi": "3.0.3",
        "info": {
            "title": "Uno Payment Gateway",
            "description": "Payment gateway with idempotency, state machine transitions, and crash recovery",
            "version": "0.1.0"
        },
        "servers": [
            {
                "url": "http://localhost:8080",
                "description": "Local development server"
            }
        ],
        "tags": [
            {
                "name": "payments",
                "description": "Payment authorization, capture, void, and refund operations"
            }
        ],
        "paths": {
            "/payments/authorize": {
                "post": {
                    "tags": ["payments"],
                    "summary": "Authorize a payment",
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "$ref": "#/components/schemas/AuthorizeRequest"
                                }
                            }
                        }
                    },
                    "responses": {
                        "200": {
                            "description": "Payment authorized",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "$ref": "#/components/schemas/AuthorizeResponse"
                                    }
                                }
                            }
                        },
                        "400": {
                            "description": "Bad request",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "$ref": "#/components/schemas/ErrorResponse"
                                    }
                                }
                            }
                        },
                        "500": {
                            "description": "Internal server error",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "$ref": "#/components/schemas/ErrorResponse"
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "/payments/{id}/capture": {
                "post": {
                    "tags": ["payments"],
                    "summary": "Capture an authorized payment",
                    "parameters": [
                        {
                            "name": "id",
                            "in": "path",
                            "required": true,
                            "schema": {
                                "type": "string",
                                "format": "uuid"
                            }
                        }
                    ],
                    "parameters_header": {
                        "$ref": "#/components/parameters/IdempotencyKey"
                    },
                    "responses": {
                        "200": {
                            "description": "Payment captured"
                        },
                        "400": {
                            "description": "Bad request",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "$ref": "#/components/schemas/ErrorResponse"
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "/payments/{id}/void": {
                "post": {
                    "tags": ["payments"],
                    "summary": "Void an authorized payment",
                    "parameters": [
                        {
                            "name": "id",
                            "in": "path",
                            "required": true,
                            "schema": {
                                "type": "string",
                                "format": "uuid"
                            }
                        }
                    ],
                    "parameters_header": {
                        "$ref": "#/components/parameters/IdempotencyKey"
                    },
                    "responses": {
                        "200": {
                            "description": "Payment voided"
                        },
                        "400": {
                            "description": "Bad request",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "$ref": "#/components/schemas/ErrorResponse"
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "/payments/{id}/refund": {
                "post": {
                    "tags": ["payments"],
                    "summary": "Refund a captured payment",
                    "parameters": [
                        {
                            "name": "id",
                            "in": "path",
                            "required": true,
                            "schema": {
                                "type": "string",
                                "format": "uuid"
                            }
                        }
                    ],
                    "parameters_header": {
                        "$ref": "#/components/parameters/IdempotencyKey"
                    },
                    "responses": {
                        "200": {
                            "description": "Payment refunded"
                        },
                        "400": {
                            "description": "Bad request",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "$ref": "#/components/schemas/ErrorResponse"
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "/payments/{id}": {
                "get": {
                    "tags": ["payments"],
                    "summary": "Get a payment",
                    "parameters": [
                        {
                            "name": "id",
                            "in": "path",
                            "required": true,
                            "schema": {
                                "type": "string",
                                "format": "uuid"
                            }
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "Payment details",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "$ref": "#/components/schemas/PaymentSummary"
                                    }
                                }
                            }
                        },
                        "404": {
                            "description": "Payment not found"
                        }
                    }
                }
            },
            "/orders/{order_id}/payments": {
                "get": {
                    "tags": ["payments"],
                    "summary": "Get payment by order ID",
                    "parameters": [
                        {
                            "name": "order_id",
                            "in": "path",
                            "required": true,
                            "schema": {
                                "type": "string"
                            }
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "Payment details",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "$ref": "#/components/schemas/PaymentSummary"
                                    }
                                }
                            }
                        },
                        "404": {
                            "description": "Payment not found"
                        }
                    }
                }
            },
            "/customers/{customer_id}/payments": {
                "get": {
                    "tags": ["payments"],
                    "summary": "List payments by customer ID",
                    "parameters": [
                        {
                            "name": "customer_id",
                            "in": "path",
                            "required": true,
                            "schema": {
                                "type": "string"
                            }
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "List of payments",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "array",
                                        "items": {
                                            "$ref": "#/components/schemas/PaymentSummary"
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
        "components": {
            "schemas": {
                "AuthorizeRequest": {
                    "type": "object",
                    "required": ["order_id", "customer_id", "amount_cents", "card_number", "cvv", "card_last_four", "card_expiry_month", "card_expiry_year"],
                    "properties": {
                        "order_id": { "type": "string", "description": "Order identifier" },
                        "customer_id": { "type": "string", "description": "Customer identifier" },
                        "amount_cents": { "type": "integer", "description": "Amount in cents", "example": 1000 },
                        "card_number": { "type": "string", "description": "Card number", "example": "4111111111111111" },
                        "cvv": { "type": "string", "description": "Card CVV", "example": "123" },
                        "card_last_four": { "type": "string", "description": "Last four digits", "example": "1111" },
                        "card_expiry_month": { "type": "integer", "description": "Expiry month", "example": 12 },
                        "card_expiry_year": { "type": "integer", "description": "Expiry year", "example": 2026 },
                        "idempotency_key": { "type": "string", "description": "Idempotency key (optional)" }
                    }
                },
                "AuthorizeResponse": {
                    "type": "object",
                    "properties": {
                        "payment_id": { "type": "string", "description": "Payment UUID" },
                        "state": { "type": "string", "description": "Payment state" }
                    }
                },
                "PaymentSummary": {
                    "type": "object",
                    "properties": {
                        "payment_id": { "type": "string", "description": "Payment UUID" },
                        "order_id": { "type": "string", "description": "Order identifier" },
                        "customer_id": { "type": "string", "description": "Customer identifier" },
                        "amount_cents": { "type": "integer", "description": "Amount in cents" },
                        "currency": { "type": "string", "description": "Currency code", "example": "USD" },
                        "state": { "type": "string", "description": "Payment state" },
                        "bank_reference": { "type": "string", "nullable": true, "description": "Bank reference" },
                        "last_four": { "type": "string", "nullable": true, "description": "Card last four digits" }
                    }
                },
                "ErrorResponse": {
                    "type": "object",
                    "required": ["error"],
                    "properties": {
                        "error": { "type": "string", "description": "Error message" }
                    }
                }
            },
            "parameters": {
                "IdempotencyKey": {
                    "name": "Idempotency-Key",
                    "in": "header",
                    "description": "Idempotency key for idempotent requests",
                    "required": false,
                    "schema": {
                        "type": "string"
                    }
                }
            }
        }
    }))
}

/// Serve the Swagger UI HTML page.
pub async fn swagger_ui() -> impl axum::response::IntoResponse {
    axum::response::Html(r##"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Uno Payment Gateway - Swagger UI</title>
    <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
</head>
<body>
    <div id="swagger-ui"></div>
    <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
    <script>
        window.onload = () => {
            const ui = SwaggerUIBundle({
                url: "/openapi.json",
                dom_id: "#swagger-ui",
                presets: [
                    SwaggerUIBundle.presets.apis,
                ],
            });
        };
    </script>
</body>
</html>"##)
}
