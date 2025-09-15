#!/bin/bash

# This script handles the --help flag properly for Docker containers

# Check if the first argument is --help or -h
if [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
    python main.py --help
    exit 0
fi

# Otherwise, pass all arguments to the main Python script
exec python main.py "$@"
