# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Terminal utility functions."""

from __future__ import annotations

import os
import re
import textwrap

from utils.logger import setup_clean_logger


def get_terminal_width() -> int:
    """Get current terminal width."""
    try:
        return os.get_terminal_size().columns
    except OSError:
        return 80  # Default width if unable to detect


def colorize_tags(text: str) -> str:
    """Apply different colors to different types of tags in text."""
    colors = {
        'red': '\033[31m',
        'yellow': '\033[33m',
        'green': '\033[32m',
        'blue': '\033[34m',
        'magenta': '\033[35m',
        'reset': '\033[0m',
    }

    text = re.sub(r'\[PATIENT\]', f'{colors["red"]}[PATIENT]{colors["reset"]}', text)
    text = re.sub(r'\[PERSOON-(\d+)\]', f'{colors["yellow"]}[PERSOON-\\1]{colors["reset"]}', text)
    text = re.sub(r'\[(DATUM|DATE)-(\d+)\]', f'{colors["green"]}[\\1-\\2]{colors["reset"]}', text)
    text = re.sub(r'\[(LOCATIE|LOCATION|PLAATS)-(\d+)\]', f'{colors["blue"]}[\\1-\\2]{colors["reset"]}', text)

    return re.sub(r'\[(TELEFOON|PHONE|EMAIL|BSN)-(\d+)\]', f'{colors["magenta"]}[\\1-\\2]{colors["reset"]}', text)


def log_block(title: str, content: str | dict) -> None:
    """Log content in a nice formatted block for improved log readability."""
    logger = setup_clean_logger()
    width = get_terminal_width()

    # Count log blocks (title)
    if not hasattr(log_block, 'counter'):
        log_block.counter = 0
    log_block.counter += 1

    # Unicode box drawing characters
    top_line = '┌' + '─' * (width - 2) + '┐'
    bottom_line = '└' + '─' * (width - 2) + '┘'
    separator_line = '├' + '─' * (width - 2) + '┤'

    # Header box with automatic report numbering
    header_title = f'{title.upper()} {log_block.counter}'

    title_padding = (width - len(header_title) - 2) // 2  # Align title to center
    header = f'│{" " * title_padding}{header_title}{" " * (width - len(header_title) - title_padding - 2)}│'

    # Log the header box to terminal
    logger.info(top_line)
    logger.info(header)
    logger.info(separator_line)

    # Log the content into box to terminal
    section_count = len(content)

    for i, (section_name, text) in enumerate(content.items()):
        section_header = f'│  {section_name}:'
        section_header += ' ' * (width - len(section_header) - 1) + '│'
        logger.info(section_header)

        # Wrap and indent text
        wrapped_lines = textwrap.wrap(str(text), width=width - 8)
        for line_text in wrapped_lines:
            visible_length = len(re.sub(r'\033\[[0-9;]*m', '', line_text))
            content_line = f'│     {line_text}'
            visible_content_length = 6 + visible_length
            padding_needed = width - visible_content_length - 1
            content_line += ' ' * padding_needed + '│'
            logger.info(content_line)

        # Empty line between sections (except after last section)
        if i < section_count - 1:
            empty_line = '│' + ' ' * (width - 2) + '│'
            logger.info(empty_line)

    # Log the footer box to terminal
    logger.info(bottom_line)
    logger.info('')
