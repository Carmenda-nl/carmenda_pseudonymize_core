# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Request and response schemas for the pseudonymization API."""

from typing import Annotated, Any

from fastapi import File, Form, UploadFile
from pydantic import BaseModel

FileField = Annotated[
    UploadFile,
    File(description='The report file to process in the supported formats.'),
]
OptionalFileField = Annotated[
    UploadFile | None,
    File(description='Optional datakey for consistent pseudonymization across sessions.'),
]
InputCols = Annotated[
    str,
    Form(
        description="Comma-separated column mappings in key=value format. At least one 'report' key is required.",
        json_schema_extra={'example': 'report=Report, clientname=Patient'},
    ),
]


class InfoResponse(BaseModel):
    """Health check & app info response."""

    status: str
    host: str
    port: int
    debug: str
    log_level: str


class StatusResponse(BaseModel):
    """Simple status response."""

    status: str


class MetricsSchema(BaseModel):
    """Timing and row-count metrics for a completed pseudonymization run."""

    total_rows: int
    hours: int
    minutes: int
    seconds: int
    time_per_row: float


class ProcessResponse(BaseModel):
    """Result payload returned after a completed pseudonymization job."""

    preview: list[dict[str, Any]]
    metrics: MetricsSchema
    output_url: str
    datakey_url: str | None = None
    log_url: str | None = None


class ProgressResponse(BaseModel):
    """Progress payload reporting the current state of an ongoing pseudonymization job."""

    stage: str | None
    percentage: int
    rows_total: int | None = None
    rows_processed: int | None = None
