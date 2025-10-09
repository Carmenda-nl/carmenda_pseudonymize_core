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

    async def connect(self) -> None:
        """Handle WebSocket connection from client."""
        self.job_id = self.scope['url_route']['kwargs']['job_id']
        self.room_group_name = f'job_progress_{self.job_id}'


        await self.channel_layer.group_add(self.room_group_name, self.channel_name)
        await self.accept()

        logger.debug('WebSocket connected to job %s', self.job_id)

        # Send current job status immediately upon connection
        try:
            job = await self.get_job(self.job_id)
            
            # Get progress from tracker if job is processing
            if job.status == 'processing':
                progress_info = tracker.get_progress()
                current_progress = progress_info['percentage']
                current_stage = progress_info['stage'] or job.status
            else:
                current_progress = 100 if job.status == 'completed' else 0
                current_stage = job.status
            
            await self.send(text_data=json.dumps({
                'type': 'progress_update',
                'percentage': current_progress,
                'stage': current_stage,
                'status': job.status,
            }))
        except DeidentificationJob.DoesNotExist:
            await self.send(text_data=json.dumps({
                'type': 'error',
                'message': f'Job {self.job_id} not found',
            }))
            await self.close()

    async def disconnect(self, close_code: int) -> None:
        """Handle WebSocket disconnection."""
        logger.info('[WS] Client disconnected from job %s (code: %s)', self.job_id, close_code)

        # Leave room group
        await self.channel_layer.group_discard(
            self.room_group_name,
            self.channel_name,
        )

    async def receive(self, text_data: str) -> None:
        """Receive message from WebSocket client.
        
        This can be used for bidirectional communication if needed,
        e.g., to pause/resume jobs, cancel processing, etc.
        """
        try:
            data = json.loads(text_data)
            command = data.get('command')

            if command == 'ping':
                await self.send(text_data=json.dumps({
                    'type': 'pong',
                    'timestamp': data.get('timestamp'),
                }))
            else:
                logger.warning('[WS] Unknown command from client: %s', command)

        except json.JSONDecodeError:
            logger.error('[WS] Invalid JSON received from client')

    async def job_progress(self, event: dict) -> None:
        """Receive progress update from channel layer and send to WebSocket client.
        
        This method is called when a message is sent to the channel layer
        with type='job_progress' (from views.py).
        """
        # Send progress update to WebSocket
        await self.send(text_data=json.dumps({
            'type': 'progress_update',
            'percentage': event['percentage'],
            'stage': event['stage'],
            'status': event.get('status', 'processing'),
        }))

    @database_sync_to_async
    def get_job(self, job_id: str) -> DeidentificationJob:
        """Fetch job from database (async wrapper)."""
        return DeidentificationJob.objects.get(pk=job_id)
