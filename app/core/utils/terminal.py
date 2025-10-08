# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Terminal utility functions using Rich library."""

from __future__ import annotations

import re

from rich.console import Console
from rich.panel import Panel
from rich.text import Text


# Global console instance
console = Console()


def get_separator_line(char: str = '-', padding: int = 0) -> None:  # noqa: ARG001
    """Print a separator line using Rich Rule.

    Note: This now prints directly instead of returning a string.
    For backwards compatibility, use console.rule() instead.
    """
    console.rule(style='dim')


def colorize_tags(text: str) -> Text:
    """Apply Rich styling to different types of tags in text.

    Returns a Rich Text object with colored tags.
    """
    # Create Rich Text object
    rich_text = Text()

    # Pattern to match all tags
    tag_pattern = (
        r'\[(PATIENT|PERSOON-\d+|(?:DATUM|DATE)-\d+|'
        r'(?:LOCATIE|LOCATION|PLAATS)-\d+|(?:TELEFOON|PHONE|EMAIL|BSN)-\d+)\]'
    )

    last_end = 0
    for match in re.finditer(tag_pattern, text):
        # Add text before the tag
        rich_text.append(text[last_end:match.start()])

        tag = match.group(0)
        tag_type = match.group(1)

        # Determine color based on tag type
        if 'PATIENT' in tag_type:
            color = 'red'
        elif 'PERSOON' in tag_type:
            color = 'yellow'
        elif any(x in tag_type for x in ['DATUM', 'DATE']):
            color = 'green'
        elif any(x in tag_type for x in ['LOCATIE', 'LOCATION', 'PLAATS']):
            color = 'blue'
        else:  # TELEFOON, PHONE, EMAIL, BSN
            color = 'magenta'

        # Add colored tag
        rich_text.append(tag, style=color)
        last_end = match.end()

    # Add remaining text
    rich_text.append(text[last_end:])

    return rich_text


# Counter for log blocks
_log_block_counter = 0


def log_block(title: str, content: dict) -> None:
    """Log content in a nice formatted Rich Panel for improved readability.

    Args:
        title: Title of the panel
        content: Dictionary with section names as keys and content as values

    """
    global _log_block_counter  # noqa: PLW0603
    _log_block_counter += 1

    # Build panel content
    panel_content = Text()

    for i, (section_name, text) in enumerate(content.items()):
        # Add section header
        panel_content.append(f'{section_name}:\n', style='bold cyan')

        # Add section content (handle both Text and str)
        if isinstance(text, Text):
            panel_content.append(text)
        else:
            panel_content.append(str(text))

        # Add spacing between sections (except for last one)
        if i < len(content) - 1:
            panel_content.append('\n\n')

    # Create and print panel
    panel = Panel(
        panel_content,
        title=f'[bold]{title} {_log_block_counter}[/bold]',
        border_style='white',
        padding=(1, 2),
    )

    # Check if there's an active progress tracker to avoid console conflicts
    from .progress_tracker import tracker

    if tracker.rich_progress is not None:
        # Print newline first, then use progress.console.print() to avoid interfering with progress bar
        tracker.rich_progress.console.print()  # Empty line before panel
        tracker.rich_progress.console.print(panel)
        tracker.rich_progress.console.print()  # Empty line after panel
    else:
        # No active progress, use regular console
        console.print()  # Empty line before panel
        console.print(panel)
        console.print()  # Empty line after panel
