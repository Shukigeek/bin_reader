import pytest
import struct
from src.business_logic.process import MAVParserProcess, HEADER, FMT_HEADER, FMT_LENGTH, STRING_FORMATS

# -------------------------
# Fixtures
# -------------------------
@pytest.fixture
def sample_file(tmp_path):
    """Create a small sample binary file with a dummy FMT message."""
    path = tmp_path / "test_log.bin"
    with open(path, "wb") as f:
        # Write a dummy FMT header
        f.write(FMT_HEADER + bytes(FMT_LENGTH - len(FMT_HEADER)))
        # Add a dummy data message with header
        f.write(bytes(HEADER) + bytes([1]) + bytes(10))  # type=1, payload=10 bytes
    return str(path)


@pytest.fixture
def parser(sample_file):
    return MAVParserProcess(sample_file)


# -------------------------
# Test _parse_fmt
# -------------------------
def test_parse_fmt(parser):
    # Create dummy chunk
    dummy_chunk = bytearray(FMT_LENGTH)
    # Put type, length, name, fmt, cols
    struct.pack_into("<BB4s16s64s", dummy_chunk, 3, 1, FMT_LENGTH, b"TEST", b"fff", b"A,B,C")
    parser._parse_fmt(dummy_chunk)
    assert 1 in parser.fmts
    fmt_info = parser.fmts[1]
    assert fmt_info["Name"] == "TEST"
    assert fmt_info["Length"] == FMT_LENGTH
    assert fmt_info["Columns"] == ["A", "B", "C"]


# -------------------------
# Test _prepare_safe_chunks
# -------------------------
def test_prepare_safe_chunks(parser):
    parser._parse_fmt(bytearray(FMT_LENGTH))  # dummy FMT
    parser._prepare_safe_chunks()
    assert isinstance(parser.chunks, list)
    assert all(len(chunk) == 2 for chunk in parser.chunks)


# -------------------------
# Test _parse_message
# -------------------------
def test_parse_message(parser):
    fmt_info = {
        "Name": "TEST",
        "Format": "fff",
        "Columns": ["A", "B", "C"],
        "CombinedFmt": "<fff",
        "Scaling": {},
        "Rounding": set(),
    }
    dummy_bytes = struct.pack("<fff", 1.0, 2.0, 3.0)
    message = parser._parse_message(fmt_info, dummy_bytes, 0, set())
    assert message["mavpackettype"] == "TEST"
    assert message["A"] == 1.0
    assert message["B"] == 2.0
    assert message["C"] == 3.0


# -------------------------
# Test _process_chunk
# -------------------------
def test_process_chunk(parser, sample_file):
    # Setup dummy FMT
    parser._parse_fmt(bytearray(FMT_LENGTH))
    chunk = (0, 20)
    args = (0, sample_file, chunk, parser.fmts, None, True)
    index, messages = MAVParserProcess._process_chunk(args)
    assert index == 0
    assert isinstance(messages, list)


# -------------------------
# Test scan_file_and_prepare_chunks
# -------------------------
def test_scan_file_and_prepare_chunks(parser):
    parser.scan_file_and_prepare_chunks()
    assert isinstance(parser.fmts, dict)
    assert isinstance(parser.chunks, list)


# -------------------------
# Test run
# -------------------------
def test_run(parser):
    parser.run()
    assert isinstance(parser.messages, list)
    assert hasattr(parser, "message_count")
    assert parser.message_count == len(parser.messages)
