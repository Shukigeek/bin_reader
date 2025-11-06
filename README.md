bin_reader

A fast Python tool for reading and parsing MAVLink .bin log files.

Features

Three parsing modes:

pymavlink – standard reference implementation

linear – single-pass custom parser (optimized for speed)

threads / processes – parallel parsing for large files

Built-in speed and reliability tests to compare performance between modes

Uses mmap and memoryview for ultra-fast binary access

Usage
git clone https://github.com/Shukigeek/bin_reader.git
cd bin_reader


Example:

from src.business_logic.mav_parser_linear import MAVParserLinear

parser = MAVParserLinear("path/to/log.bin")
messages = parser.parse()
