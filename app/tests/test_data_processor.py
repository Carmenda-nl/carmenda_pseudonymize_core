# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Unit tests for data_processor.py module."""

import pytest
from unittest.mock import patch, MagicMock

from core.data_processor import process_data


class TestDataProcessor:
    """Test the data processing functionality."""

    @patch('core.data_processor.DeduceHandler')
    @patch('core.data_processor.pseudonymize')
    @patch('builtins.print')  # Mock print to avoid output
    @patch('core.data_processor.tracker')
    @patch('core.data_processor.progress_tracker')
    @patch('core.data_processor.get_file_size')
    @patch('core.data_processor.get_file_extension')
    @patch('core.data_processor.setup_logging')
    @patch('core.data_processor.save_data_file')
    @patch('core.data_processor.create_output_file_path')
    @patch('core.data_processor.save_pseudonym_key')
    @patch('core.data_processor.load_pseudonym_key')
    @patch('core.data_processor.load_data_file')
    @patch('core.data_processor.get_environment_paths')
    def test_process_data_basic_functionality(
        self,
        mock_get_env_paths,
        mock_load_data,
        mock_load_pseudonym_key,
        mock_save_pseudonym_key,
        mock_create_output_path,
        mock_save_data,
        mock_setup_logging,
        mock_get_file_extension,
        mock_get_file_size,
        mock_progress_tracker,
        mock_tracker,
        mock_print,
        mock_pseudonymize,
        mock_deduce_handler_class,
    ):
        """Test basic functionality of process_data function."""
        # Setup mock return values
        mock_get_env_paths.return_value = ('/input/path', '/output/path')
        
        # Create a mock DataFrame with proper columns attribute
        mock_df = MagicMock()
        mock_df.columns = ['name', 'text']  # Columns that match input_cols mapping
        mock_df.height = 5
        mock_df.with_columns.return_value = mock_df
        mock_df.drop.return_value = mock_df
        mock_df.filter.return_value = mock_df
        mock_df.select.return_value = mock_df
        mock_df.rename.return_value = mock_df
        mock_df.to_dict.return_value = {'name': ['John'], 'text': ['Test report']}
        mock_df.to_dicts.return_value = [{'name': 'John', 'text': 'Test report'}]  # Add this for JSON serialization
        mock_df.head.return_value = mock_df
        
        mock_load_data.return_value = mock_df
        mock_load_pseudonym_key.return_value = {}
        mock_create_output_path.return_value = '/output/test.parquet'
        mock_get_file_extension.return_value = '.csv'
        mock_get_file_size.return_value = '1MB'
        mock_progress_tracker.return_value = {
            'update': MagicMock(),
            'get_stage_name': MagicMock(return_value='Stage'),
        }
        
        # Mock pseudonymize function
        mock_pseudonymize.return_value = ({'John Doe': 'PATIENT_001'}, mock_df)
        
        # Mock DeduceHandler
        mock_handler = MagicMock()
        mock_handler.deduce_with_id.return_value = MagicMock()
        mock_handler.replace_tags.return_value = 'Processed text'
        mock_deduce_handler_class.return_value = mock_handler
        
        # Test parameters
        input_fofi = 'test.csv'
        input_cols = 'patientName=name,report=text'
        output_cols = 'patientID=id,processed_report=processed'
        pseudonym_key = None
        output_extension = '.parquet'
        
        # Call the function
        result = process_data(
            input_fofi=input_fofi,
            input_cols=input_cols,
            output_cols=output_cols,
            pseudonym_key=pseudonym_key,
            output_extension=output_extension,
        )
        
        # Verify function calls
        mock_get_env_paths.assert_called_once()
        mock_load_data.assert_called_once()
        
        # Verify result is a string (output path)
        assert isinstance(result, str)

    @patch('core.data_processor.get_environment_paths')
    def test_process_data_invalid_input_file(self, mock_get_env_paths):
        """Test process_data with invalid input file."""
        mock_get_env_paths.return_value = ('/input/path', '/output/path')
        
        # This should raise an exception when trying to load non-existent file
        with pytest.raises(Exception):
            process_data(
                input_fofi='nonexistent.csv',
                input_cols='patientName=name',
                output_cols='patientID=id',
                pseudonym_key=None,
                output_extension='.parquet',
            )


if __name__ == '__main__':
    pytest.main([__file__])
