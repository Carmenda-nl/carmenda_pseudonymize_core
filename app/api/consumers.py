# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""WebSocket consumers for real-time communication."""

from __future__ import annotations

import json
import logging

from channels.db import database_sync_to_async
from channels.generic.websocket import AsyncWebsocketConsumer

from api.models import DeidentificationJob
from core.utils.progress_tracker import tracker

logger = logging.getLogger(__name__)


class JobProgressConsumer(AsyncWebsocketConsumer):
    """WebSocket consumer for real-time job progress updates."""

    connected: bool = False

    @database_sync_to_async
    def get_job(self, job_id: str) -> DeidentificationJob:
        """Fetch job from database (async wrapper)."""
        return DeidentificationJob.objects.get(pk=job_id)

    async def connect(self) -> None:
        """Handle WebSocket connection from client."""
        self.job_id = self.scope['url_route']['kwargs']['job_id']
        self.room_group_name = f'job_progress_{self.job_id}'

        await self.channel_layer.group_add(self.room_group_name, self.channel_name)
        await self.accept()

        self.connected = True
        logger.debug('WebSocket connected to job %s', self.job_id)

        try:
            job = await self.get_job(self.job_id)

            if job.status == 'processing':
                progress_info = tracker.get_progress()
                current_progress = progress_info['percentage']
                current_stage = progress_info['stage'] or job.status
            else:
                current_progress = 100 if job.status == 'completed' else 0
                current_stage = job.status

            await self.safe_send(
                text_data=json.dumps(
                    {
                        'type': 'progress_update',
                        'percentage': current_progress,
                        'stage': current_stage,
                        'status': job.status,
                    },
                ),
            )

        except DeidentificationJob.DoesNotExist:
            await self.safe_send(text_data=json.dumps({'type': 'error', 'message': f'Job {self.job_id} not found'}))
            await self.close()

    async def disconnect(self, close_code: int) -> None:
        """Handle WebSocket disconnection from client."""
        self.connected = False

        logger.debug('Client disconnected from job %s (code: %s)', self.job_id, close_code)
        await self.channel_layer.group_discard(self.room_group_name, self.channel_name)

    async def safe_send(self, *, text_data: str | None = None, bytes_data: bytes | None = None) -> bool:
        """Send to the websocket while guarding against closed-protocol errors."""
        if not self.connected:
            logger.debug('Skipping send for job %s because socket is disconnected', getattr(self, 'job_id', None))
            return False

        try:
            await self.send(text_data=text_data, bytes_data=bytes_data)
        except (RuntimeError, OSError):
            logger.debug('Failed to send websocket message for job %s', getattr(self, 'job_id', None), exc_info=True)
            return False
        else:
            return True

    async def job_progress(self, event: dict) -> None:
        """Receive progress update from channel layer and send to WebSocket client."""
        if not getattr(self, 'connected', False):
            logger.debug(
                'Dropping progress update for job %s because socket is disconnected',
                getattr(self, 'job_id', None),
            )
            return

        await self.safe_send(
            text_data=json.dumps(
                {
                    'type': 'progress_update',
                    'percentage': event['percentage'],
                    'stage': event['stage'],
                    'status': event.get('status', 'processing'),
                },
            ),
        )
