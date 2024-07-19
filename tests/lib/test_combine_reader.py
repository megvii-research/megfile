import os
from io import BufferedWriter, BytesIO, StringIO

import pytest

from megfile.lib.combine_reader import CombineReader


def test_combine_reader(fs):
    reader = CombineReader([BytesIO(b"block0")], "test_combine_reader")
    assert reader.name == "test_combine_reader"
    assert reader.mode == "rb+"

    with pytest.raises(IOError):
        CombineReader([None], "test_combine_reader_2")

    with pytest.raises(IOError):
        CombineReader([BufferedWriter(BytesIO(b"block0 "))], "test_combine_reader_3")

    with pytest.raises(IOError):
        CombineReader([BytesIO(b""), StringIO("")], "test_combine_reader_4")


def test_combine_reader_read():
    block0 = BytesIO(b"block0 ")
    block1 = BytesIO(b"block1 ")
    block2 = BytesIO(b"block2 ")
    block3 = BytesIO(b"block3 ")
    block4 = BytesIO(b"block4 ")
    reader = CombineReader(
        [block0, block1, block2, block3, block4], "test_combine_reader"
    )

    # size = 0
    assert reader.read(0) == b""
    assert reader.tell() == 0

    # In-block read
    assert reader.read(2) == b"bl"
    assert reader.tell() == 2

    # Cross-block read
    assert reader.read(6) == b"ock0 b"

    assert reader.read(6) == b"lock1 "

    # 连续读多个 block, 且 size 超过剩余数据大小
    assert reader.read(21 + 1) == b"block2 block3 block4 "

    # Seek to head then read
    reader.seek(0)
    assert reader.tell() == 0
    assert reader.read() == b"block0 block1 block2 block3 block4 "
    assert reader.tell() == 35

    with CombineReader(
        [block0, block1, block2, block3, block4], "test_combine_reader_1"
    ) as reader:
        assert reader.read() == b"block0 block1 block2 block3 block4 "


def test_combine_reader_read_stringIO():
    block0 = StringIO("block0 ")
    block1 = StringIO("block1 ")
    block2 = StringIO("block2 ")
    block3 = StringIO("block3 ")
    block4 = StringIO("block4 ")
    reader = CombineReader(
        [block0, block1, block2, block3, block4], "test_combine_reader"
    )

    # size = 0
    assert reader.read(0) == ""
    assert reader.tell() == 0

    # In-block read
    assert reader.read(2) == "bl"
    assert reader.tell() == 2

    # Cross-block read
    assert reader.read(6) == "ock0 b"

    assert reader.read(6) == "lock1 "

    # 连续读多个 block, 且 size 超过剩余数据大小
    assert reader.read(21 + 1) == "block2 block3 block4 "

    # Seek to head then read
    reader.seek(0)
    assert reader.tell() == 0
    assert reader.read() == "block0 block1 block2 block3 block4 "
    assert reader.tell() == 35

    reader.seek(36)
    assert reader.read() == ""

    with CombineReader(
        [block0, block1, block2, block3, block4], "test_combine_reader_1"
    ) as reader:
        assert reader.read() == "block0 block1 block2 block3 block4 "

    with pytest.raises(ValueError):
        reader.seek(-1)

    with pytest.raises(ValueError):
        reader.seek(0, 100)


def test_combine_reader_read_text():
    block0 = StringIO("block0 ")
    block1 = StringIO("block1 ")
    block2 = StringIO("block2 ")
    block3 = StringIO("block3 ")
    block4 = StringIO("block4 ")
    reader = CombineReader(
        [block0, block1, block2, block3, block4], "test_combine_reader"
    )
    assert reader.mode == "r+"

    # size = 0
    assert reader.read(0) == ""

    # block 内读
    assert reader.read(2) == "bl"

    # 跨 block 读
    assert reader.read(6) == "ock0 b"

    assert reader.read(6) == "lock1 "

    # 连续读多个 block, 且 size 超过剩余数据大小
    assert reader.read(21 + 1) == "block2 block3 block4 "

    # 从头再读
    reader.seek(0)
    assert reader.read() == "block0 block1 block2 block3 block4 "

    with CombineReader(
        [block0, block1, block2, block3, block4], "test_combine_reader_1"
    ) as reader:
        assert reader.read() == "block0 block1 block2 block3 block4 "


def test_combine_reader_readline():
    io1 = BytesIO(b"1\n2\n3\n\n4444\n5")
    io2 = BytesIO(b" 6666666666666666666 ")
    io3 = BytesIO(b"\n7 7 7")

    file_objects = [io1, io2, io3]

    reader = CombineReader(file_objects, "test_combine_reader_readline")
    # within block
    assert reader.readline() == b"1\n"
    # cross block
    assert reader.readline() == b"2\n"
    # remaining is enough
    assert reader.readline() == b"3\n"
    # single line break
    assert reader.readline() == b"\n"
    # more than one block
    assert reader.readline() == b"4444\n"
    # tailing bytes
    assert reader.readline() == b"5 6666666666666666666 \n"

    assert reader.readline() == b"7 7 7"


def test_combine_reader_readline_without_line_break_at_all():
    io1 = BytesIO(b"123 456 789")
    io2 = BytesIO(b"987 654 321")
    with CombineReader(
        [io1, io2], "test_combine_reader_readline_without_line_break_at_all"
    ) as reader:
        reader.read(1)
        assert reader.readline() == b"23 456 789987 654 321"


def test_combine_reader_read_readline_mix():
    io = BytesIO(b"1\n2\n3\n4\n")
    io_list = []
    for index in range(2):
        io_list.append(io)

    with CombineReader(io_list, "test_combine_reader_read_readline_mix") as reader:
        assert reader.readline() == b"1\n"
        assert reader.read(2) == b"2\n"
        assert reader.readline() == b"3\n"
        assert reader.read(1) == b"4"
        assert reader.readline() == b"\n"
        assert reader.readline() == b"1\n"
        assert reader.read(2) == b"2\n"
        assert reader.readline() == b"3\n"
        assert reader.read(1) == b"4"
        assert reader.readline() == b"\n"
        assert reader.readline() == b""
        assert reader.read() == b""


def test_combine_reader_seek():
    io1 = BytesIO(b"123 456 789 \n")
    io2 = BytesIO(b"\n987 654 321")
    io3 = BytesIO(b"=======")
    with CombineReader([io1, io2, io3], "test_combine_reader_seek") as reader:
        reader.seek(0)

        assert reader.read(7) == b"123 456"
        reader.seek(7)
        reader.seek(0, os.SEEK_CUR)
        reader.seek(-28, os.SEEK_END)

        reader.seek(-1, os.SEEK_CUR)
        reader.seek(0, os.SEEK_CUR)
        reader.seek(1, os.SEEK_CUR)

        reader.seek(-1, os.SEEK_END)
        reader.seek(0, os.SEEK_END)
        reader.seek(1, os.SEEK_END)


def test_combine_reader_read_with_forward_seek():
    io1 = BytesIO(b"123 456 789 \n")
    io2 = BytesIO(b"\n987 654 321")
    io3 = BytesIO(b"======= ")
    reader = CombineReader(
        [io1, io2, io3], "test_combine_reader_read_with_forward_seek_1"
    )
    reader.seek(3)
    assert reader.read(4) == b" 456"
    reader = CombineReader(
        [io1, io2, io3], "test_combine_reader_read_with_forward_seek_2"
    )
    reader.read(1)
    reader.seek(4)
    assert reader.read(4) == b"456 "
    reader = CombineReader(
        [io1, io2, io3], "test_combine_reader_read_with_forward_seek_3"
    )
    reader.seek(13)  # 目标 offset 距当前位置正好为一个 block 大小
    assert reader.read(12) == b"\n987 654 321"
    reader = CombineReader(
        [io1, io2, io3], "test_combine_reader_read_with_forward_seek_4"
    )
    reader.read(1)
    reader.seek(12)
    assert reader.read(14) == b"\n\n987 654 321="
    reader = CombineReader(
        [io1, io2, io3], "test_combine_reader_read_with_forward_seek_5"
    )
    reader.seek(25)
    assert reader.read(8) == b"======= "
    with CombineReader(
        [io1, io2, io3], "test_combine_reader_read_with_forward_seek_6"
    ) as reader:
        reader.seek(-1, os.SEEK_END)
        assert reader.read(2) == b" "


def test_combine_reader_tell():
    block0 = BytesIO(b"block0 ")
    block1 = BytesIO(b"block1 ")
    block2 = BytesIO(b"block2 ")
    block3 = BytesIO(b"block3 ")
    block4 = BytesIO(b"block4 ")

    with CombineReader(
        [block0, block1, block2, block3, block4], "test_combine_reader_tell"
    ) as reader:
        assert reader.tell() == 0
        reader.read(0)
        assert reader.tell() == 0
        reader.read(1)
        assert reader.tell() == 1
        reader.read(6)
        assert reader.tell() == 7
        reader.read(28)
        assert reader.tell() == 35


def test_combine_reader_tell_after_seek():
    block0 = BytesIO(b"block0 ")
    block1 = BytesIO(b"block1 ")
    block2 = BytesIO(b"block2 ")
    block3 = BytesIO(b"block3 ")
    block4 = BytesIO(b"block4 ")
    with CombineReader(
        [block0, block1, block2, block3, block4], "test_combine_reader_tell_after_seek"
    ) as reader:
        reader.seek(2)
        assert reader.tell() == 2
        reader.seek(3)
        assert reader.tell() == 3
        reader.seek(13)
        assert reader.tell() == 13
        reader.seek(0, os.SEEK_END)
        assert reader.tell() == 35
