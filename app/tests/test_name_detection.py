"""Test Dutch names detection (run with pytest -s to see results)."""

import sys
from pathlib import Path

# Add the app directory to the Python path for relative imports
app_dir = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(app_dir))

from services.logger import setup_test_logging  # noqa: E402
from core.deduce_handler import DeduceHandler  # noqa: E402


def test_name_detection() -> None:
    """Test the extended detector on different sentences."""
    logger = setup_test_logging()
    handler = DeduceHandler()

    test_sentences = [
        'truus de rooij, henk de vries, en piet gingen fietsen',
        'monique naaldenberg, anja van der haar en gers wonen mooi',
        'joup, maarten van der poel, pim janssen hadden een goede dag',
    ]

    logger.info('Testing extended name detection:')
    logger.info('-' * 40)

    for sentence in test_sentences:
        logger.info(f"\nTesting: '{sentence}'")

        # Test custom detector
        custom_annotations = handler.detector.names_case_insensitive(sentence)

        if custom_annotations:
            for ann in custom_annotations:
                logger.info(f"  Found: '{ann.text}' at {ann.start}-{ann.end} (confidence: {ann.confidence:.2f})")
        else:
            logger.info('  No names detected')


if __name__ == '__main__':
    test_name_detection()
