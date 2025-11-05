import pytest
import os
import sys

from src.utils.config import FILE_PATH

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import struct
from src.business_logic.mav_parser_linear import MAVParserLinear, HEADER, FMT_TYPE, FMT_LENGTH

# -------------------------
# Fixtures
# -------------------------
@pytest.fixture
def sample_file(tmp_path):
    """Create a small sample binary file with a dummy FMT message."""
    path = tmp_path / FILE_PATH
    with open(path, "wb") as f:
        # Example header: 0xFE, 0x0E, FMT_TYPE
        header = bytes(HEADER) + bytes([FMT_TYPE])
        # Create dummy FMT payload (length = FMT_LENGTH-3 because header is 3 bytes)
        payload = bytes(FMT_LENGTH - 3)
        f.write(header + payload)
    return str(path)


@pytest.fixture
def parser(sample_file):
    return MAVParser(sample_file)


# -------------------------
# Test _build_processors
# -------------------------
def test_build_processors(parser):
    columns = ["Lat", "Lon", "Alt"]
    format_str = "fff"
    name = "GPS"
    processors = parser._build_processors(columns, format_str, name)
    assert len(processors) == 3
    for kind, col, extra in processors:
        assert kind == "numeric"
        assert col in columns


# -------------------------
# Test _parse_fmt
# -------------------------
def test_parse_fmt(parser):
    # Write a proper dummy FMT structure in memory
    # Here we simulate a simple FMT message
    offset = 0
    msg = parser._parse_fmt(offset)
    assert msg["mavpackettype"] == "FMT"
    assert "Type" in msg
    assert "Name" in msg
    assert "Format" in msg
    assert "Columns" in msg


# -------------------------
# Test _parse_message
# -------------------------
def test_parse_message(parser):
    # First, ensure a format exists
    parser.formats[1] = {
        "Name": "TEST",
        "Length": 7,
        "CompiledStruct": struct.Struct("<BfH"),
        "Processors": [("numeric", "A", (1, False)), ("numeric", "B", (1, False)), ("numeric", "C", (1, False))],
    }
    # Create dummy values to unpack
    dummy_bytes = bytes([1, 0, 0, 0, 0, 0, 0])
    parser._view = memoryview(dummy_bytes)
    msg = parser._parse_message(1, 0)
    assert msg["mavpackettype"] == "TEST"


# -------------------------
# Test _find_next_header
# -------------------------
def test_find_next_header(parser):
    pos = parser._find_next_header()
    assert pos is not None
    assert parser._view[pos] == HEADER[0]
    assert parser._view[pos + 1] == HEADER[1]


# -------------------------
# Test parse_next
# -------------------------
def test_parse_next(parser):
    msg = parser.parse_next()
    # Depending on content, may be None or FMT
    assert msg is None or "mavpackettype" in msg


# -------------------------
# Test parse_all
# -------------------------
def test_parse_all(parser):
    messages = parser.parse_all()
    # Should return a list (possibly empty)
    assert isinstance(messages, list)
    for msg in messages:
        assert "mavpackettype" in msg


# -------------------------
# Test print_summary
# -------------------------
def test_print_summary(parser, capsys):
    parser.message_count = 10
    parser.print_summary()
    captured = capsys.readouterr()
    assert "10 messages parsed" in captured.out
