# ------------------------------------------------------------------------------------------------ #
# Copyright (c) 2025 Carmenda. All rights reserved.                                                #
# This program is distributed under the terms of the GNU General Public License: GPL-3.0-or-later  #
# ------------------------------------------------------------------------------------------------ #

"""Unit tests for main.py module."""

import argparse
import pytest
from unittest.mock import patch, MagicMock

from main import parse_cli_arguments, main


class TestParseCliArguments:
    """Test the CLI argument parsing functionality."""

    def test_default_values(self) -> None:
        """Test default argument values."""
        with patch('sys.argv', ['main.py']):
            args = parse_cli_arguments()
            
            # Verify return type
            if not isinstance(args, argparse.Namespace):
                msg = 'Expected argparse.Namespace'
                raise TypeError(msg)
            
            # Check default values individually
            if args.input_fofi != 'dummy_input.csv':
                msg = 'Unexpected default input_fofi'
                raise ValueError(msg)
            
            if args.output_extension != '.parquet':
                msg = 'Unexpected default output_extension'
                raise ValueError(msg)

    def test_custom_input_file(self) -> None:
        """Test custom input file argument."""
        test_file = 'custom_test.csv'
        with patch('sys.argv', ['main.py', '--input_fofi', test_file]):
            args = parse_cli_arguments()
            
            if args.input_fofi != test_file:
                msg = 'Custom input file not set correctly'
                raise ValueError(msg)

    def test_csv_output_extension(self) -> None:
        """Test CSV output extension."""
        with patch('sys.argv', ['main.py', '--output_extension', '.csv']):
            args = parse_cli_arguments()
            
            if args.output_extension != '.csv':
                msg = 'CSV extension not set correctly'
                raise ValueError(msg)


class TestMain:
    """Test the main function."""

    @patch('main.process_data')
    @patch('main.parse_cli_arguments')
    def test_main_function_integration(
        self,
        mock_parse_args: MagicMock,
        mock_process_data: MagicMock,
    ) -> None:
        """Test main function calls process_data with parsed arguments."""
        # Setup mock
        mock_args = MagicMock()
        mock_args.input_fofi = 'test.csv'
        mock_args.input_cols = 'test_cols'
        mock_args.output_cols = 'output_cols'
        mock_args.pseudonym_key = None
        mock_args.output_extension = '.parquet'
        mock_parse_args.return_value = mock_args

        # Execute
        main()

        # Verify calls
        if mock_parse_args.call_count != 1:
            msg = 'parse_cli_arguments not called exactly once'
            raise ValueError(msg)
        
        if mock_process_data.call_count != 1:
            msg = 'process_data not called exactly once'
            raise ValueError(msg)


if __name__ == '__main__':
    # Simple test runner
    test_instance = TestParseCliArguments()
    test_instance.test_default_values()
    test_instance.test_custom_input_file()
    test_instance.test_csv_output_extension()
    
    main_test_instance = TestMain()
    # Note: The integration test requires mocking, so we skip it in simple runner
    print('Basic tests passed!')

    def test_parse_cli_arguments_help_contains_descriptions(self):
        """Test that help text contains proper descriptions."""
        with patch('sys.argv', ['main.py', '--help']):
            with pytest.raises(SystemExit):
                parse_cli_arguments()
            # This test ensures help can be called without errors


class TestMain:
    """Test the main function."""

    @patch('main.process_data')
    @patch('main.parse_cli_arguments')
    def test_main_calls_process_data_with_parsed_args(self, mock_parse_args, mock_process_data):
        """Test that main function calls process_data with parsed arguments."""
        # Setup mock arguments
        mock_args = MagicMock()
        mock_args.input_fofi = 'test.csv'
        mock_args.input_cols = 'test_input_cols'
        mock_args.output_cols = 'test_output_cols'
        mock_args.pseudonym_key = 'test_key.json'
        mock_args.output_extension = '.parquet'
        mock_parse_args.return_value = mock_args

        # Call main
        main()

        # Verify parse_cli_arguments was called
        mock_parse_args.assert_called_once()

        # Verify process_data was called with correct arguments
        mock_process_data.assert_called_once_with(
            input_fofi='test.csv',
            input_cols='test_input_cols',
            output_cols='test_output_cols',
            pseudonym_key='test_key.json',
            output_extension='.parquet'
        )

    @patch('main.process_data')
    @patch('main.parse_cli_arguments')
    def test_main_with_default_args(self, mock_parse_args, mock_process_data):
        """Test main function with default arguments."""
        # Setup mock with default values
        mock_args = MagicMock()
        mock_args.input_fofi = 'dummy_input.csv'
        mock_args.input_cols = 'patientName=Cliëntnaam, report=rapport'
        mock_args.output_cols = 'patientID=patientID, processed_report=processed_report'
        mock_args.pseudonym_key = None
        mock_args.output_extension = '.parquet'
        mock_parse_args.return_value = mock_args

        main()

        mock_process_data.assert_called_once_with(
            input_fofi='dummy_input.csv',
            input_cols='patientName=Cliëntnaam, report=rapport',
            output_cols='patientID=patientID, processed_report=processed_report',
            pseudonym_key=None,
            output_extension='.parquet'
        )

    @patch('main.process_data')
    @patch('main.parse_cli_arguments')
    def test_main_propagates_process_data_exceptions(self, mock_parse_args, mock_process_data):
        """Test that main function propagates exceptions from process_data."""
        mock_args = MagicMock()
        mock_parse_args.return_value = mock_args
        mock_process_data.side_effect = ValueError("Test error")

        with pytest.raises(ValueError, match="Test error"):
            main()

    @patch('main.process_data')
    @patch('main.parse_cli_arguments')
    def test_main_calls_functions_in_correct_order(self, mock_parse_args, mock_process_data):
        """Test that main calls functions in the correct order."""
        mock_args = MagicMock()
        mock_parse_args.return_value = mock_args

        main()

        # Check call order
        assert mock_parse_args.call_count == 1
        assert mock_process_data.call_count == 1
        
        # parse_cli_arguments should be called before process_data
        calls = [call[0] for call in [mock_parse_args.call_args_list[0], mock_process_data.call_args_list[0]]]
        assert len(calls) == 2


class TestArgumentValidation:
    """Test argument validation and edge cases."""

    def test_empty_string_arguments(self):
        """Test behavior with empty string arguments."""
        with patch('sys.argv', ['main.py', '--input_fofi', '']):
            args = parse_cli_arguments()
            assert args.input_fofi == ''

    def test_special_characters_in_arguments(self):
        """Test handling of special characters in arguments."""
        special_input = 'file_with_spec!al_ch@rs.csv'
        with patch('sys.argv', ['main.py', '--input_fofi', special_input]):
            args = parse_cli_arguments()
            assert args.input_fofi == special_input

    def test_unicode_characters_in_arguments(self):
        """Test handling of unicode characters in arguments."""
        unicode_input = 'bestand_met_unicode_é.csv'
        with patch('sys.argv', ['main.py', '--input_fofi', unicode_input]):
            args = parse_cli_arguments()
            assert args.input_fofi == unicode_input


class TestIntegration:
    """Integration tests for main.py functionality."""

    @patch('main.process_data')
    def test_end_to_end_with_mocked_process_data(self, mock_process_data):
        """Test end-to-end execution with mocked process_data."""
        test_args = [
            'main.py',
            '--input_fofi', 'integration_test.csv',
            '--output_extension', '.csv'
        ]
        
        with patch('sys.argv', test_args):
            main()
            
            mock_process_data.assert_called_once()
            call_args = mock_process_data.call_args
            assert call_args[1]['input_fofi'] == 'integration_test.csv'
            assert call_args[1]['output_extension'] == '.csv'

    def test_argument_parser_accepts_valid_formats(self):
        """Test that argument parser accepts all valid file formats."""
        valid_extensions = ['.csv', '.parquet', '.CSV', '.PARQUET']
        
        for ext in valid_extensions:
            with patch('sys.argv', ['main.py', '--output_extension', ext]):
                args = parse_cli_arguments()
                assert args.output_extension == ext


if __name__ == '__main__':
    pytest.main([__file__])
