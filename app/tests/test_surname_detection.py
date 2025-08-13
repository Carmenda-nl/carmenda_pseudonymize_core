"""Test surname detection. (run with pytest -s to see results)."""

import sys
from pathlib import Path

# Add the app directory to the Python path for relative imports
app_dir = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(app_dir))

from services.logger import setup_test_logging  # noqa: E402
from core.deduce_handler import DutchNameDetector  # noqa: E402


def test_surname_detection() -> None:
    """Test if input is properly detected as a surname."""
    logger = setup_test_logging()
    detector = DutchNameDetector()

    # Test specific names
    test_names = ['naaldenberg', 'naaldenberch', 'vroegindeweij', 'jassen', 'janssen']

    logger.info('Testing surname detection:')
    logger.info('-' * 40)

    results = []
    for name in test_names:
        is_surname = detector._surname(name)  # noqa: SLF001
        result_text = '✓ SURNAME' if is_surname else '✗ NOT SURNAME'
        logger.info('%s → %s', f'{name:<15}', result_text)
        results.append(is_surname)

    logger.info('-' * 40)
    total_surnames = sum(results)
    logger.info('Found %d surnames out of %d names tested', total_surnames, len(test_names))


if __name__ == '__main__':
    test_surname_detection()
