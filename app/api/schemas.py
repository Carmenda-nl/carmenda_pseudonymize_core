# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2026 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""FastAPI form and file field type aliases for the pseudonymization API."""

from typing import Annotated, Any

from fastapi import File, Form, UploadFile
from pydantic import BaseModel

FileField = Annotated[
    UploadFile,
    File(description='The report file to pseudonymize. Supported formats: .xlsx, .csv'),
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


class MetricsSchema(BaseModel):
    """Timing and row-count metrics for a completed pseudonymization run."""

    total_rows: int
    hours: int
    minutes: int
    seconds: int
    time_per_row: float


class ProcessResponse(BaseModel):
    """Response payload returned after a successful pseudonymization request."""

    preview: list[dict[str, Any]]
    metrics: MetricsSchema
    output_url: str
    datakey_url: str | None = None
    log_url: str | None = None


class ProgressResponse(BaseModel):
    """SSE payload reporting the current progress of an ongoing pseudonymization job."""

    stage: str | None
    percentage: int
    rows_total: int | None = None
    rows_processed: int | None = None
