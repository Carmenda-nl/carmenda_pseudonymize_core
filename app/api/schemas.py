# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Request and response schemas for the pseudonymization API."""

from typing import Annotated, Any

from fastapi import Form
from pydantic import BaseModel

JobId = Annotated[
    str,
    Form(
        description='Job ID based identifier for subfolder',
        json_schema_extra={'example': 'job id'}
    ),
]
FilePath = Annotated[
    str,
    Form(
        description='Path to the report file that needs to be processed',
        json_schema_extra={'example': 'path/to/file'}
    ),
]
InputCols = Annotated[
    str,
    Form(
        description="Comma-separated column mappings in key=value format. At least one 'report' key is required",
        json_schema_extra={'example': 'clientname=patient, report=rapport'},
    ),
]
DatakeyPath = Annotated[
    str,
    Form(description='Optional path to a datakey for consistent pseudonymization across sessions'),
]


class InfoResponse(BaseModel):
    """Health check & app info response."""

    status: str
    host: str
    port: int
    debug: bool
    log_level: str
    environment: str
    input_folder: str
    output_folder: str


class StatusResponse(BaseModel):
    """Simple status response."""

    status: str


class ErrorResponse(BaseModel):
    """Error detail payload returned for non-2xx responses."""

    detail: str


def error_responses(*responses: tuple[int, str]) -> dict[int | str, dict[str, Any]]:
    """Build an OpenAPI `responses` dict mapping status codes to ErrorResponse descriptions."""
    return {status_code: {'model': ErrorResponse, 'description': description} for status_code, description in responses}


class MetricsSchema(BaseModel):
    """Timing and row-count metrics for a completed pseudonymization run."""

    total_rows: int
    hours: int
    minutes: int
    seconds: int
    time_per_row: float


class ProcessResponse(BaseModel):
    """Result payload returned after a completed pseudonymization process."""

    preview: list[dict[str, Any]]
    metrics: MetricsSchema


class ProgressResponse(BaseModel):
    """Progress payload reporting the current state of an ongoing pseudonymization process."""

    stage: str | None
    percentage: int
    rows_total: int | None = None
    rows_processed: int | None = None
