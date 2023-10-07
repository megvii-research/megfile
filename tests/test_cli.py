import os
import sys

import pytest
from click.testing import CliRunner

from megfile.cli import cat, cp, head, ls, md5sum, mkdir, mtime, mv, s3, rm, size, stat, sync, tail, to, touch, version

from .test_smart import s3_empty_client


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def testdir(tmpdir):
    with open(str(tmpdir / 'text'), 'w') as f:
        f.write('hello')
    yield tmpdir


def test_versin(runner):
    result = runner.invoke(version)

    assert result.exit_code == 0


def test_touch(runner, tmpdir):
    result = runner.invoke(touch, [str(tmpdir / 'hello.txt')])

    assert result.exit_code == 0
    assert result.output == ''
    assert runner.invoke(ls, [str(tmpdir)]).output.endswith('hello.txt\n')


def test_mkdir(runner, tmpdir):
    result = runner.invoke(mkdir, [str(tmpdir / 'dir42')])

    assert result.exit_code == 0
    assert result.output == ''
    assert runner.invoke(ls, [str(tmpdir)]).output.endswith('dir42\n')


def test_ls(runner, testdir):
    import pdb;pdb.set_trace()
    result = runner.invoke(ls, [str(testdir)])

    assert result.exit_code == 0
    assert result.output.endswith('text\n')

    file_name = 'text'
    result_file = runner.invoke(ls, [str(testdir / file_name)])

    assert result_file.exit_code == 0
    assert result_file.output == "%s\n" % file_name

    result_file = runner.invoke(ls, ['-r', str(testdir)])

    assert result_file.exit_code == 0
    assert result_file.output == "%s\n" % file_name

    os.chdir(testdir)
    glob_result_file = runner.invoke(ls, ["*"])
    assert glob_result_file.exit_code == 0
    assert glob_result_file.output == "%s\n" % (file_name)


def test_ls_long(runner, testdir):
    result = runner.invoke(ls, ['--long', str(testdir)])

    assert result.exit_code == 0
    assert result.output.endswith('text\n')
    assert ' 5 ' in result.output


def test_ls_hunman_readable(runner, testdir):
    result = runner.invoke(ls, ['--long', '--human-readable', str(testdir)])

    assert result.exit_code == 0
    assert result.output.endswith('text\n')
    assert ' 5 B ' in result.output


def test_cat(runner, testdir):
    result = runner.invoke(cat, [str(testdir / 'text')])

    assert result.exit_code == 0
    assert result.output == 'hello'


def test_mv(runner, testdir, s3_empty_client):
    result = runner.invoke(
        mv,
        [str(testdir / 'text'), str(testdir / 'newfile')])

    assert result.exit_code == 0
    assert runner.invoke(ls, [str(testdir)]).output.endswith('newfile\n')
    assert not runner.invoke(ls, [str(testdir)]).output.endswith('text\n')

    runner.invoke(mkdir, [str(testdir / 'new_dir')])
    result_dst_path_isdir = runner.invoke(
        mv, [str(testdir / 'newfile'),
             str(testdir / 'new_dir')])

    assert result_dst_path_isdir.exit_code == 0
    assert 'newfile\n' in runner.invoke(ls, [str(testdir / 'new_dir')]).output
    assert not runner.invoke(ls, [str(testdir)]).output.endswith('newfile\n')

    runner.invoke(mkdir, [str(testdir / 'new_dir2')])
    result_dst_path_isdir = runner.invoke(
        mv, ['-r', '-g',
             str(testdir / 'new_dir'),
             str(testdir / 'new_dir2')])

    assert result_dst_path_isdir.exit_code == 0
    assert '%' in result_dst_path_isdir.output
    assert 'newfile\n' in runner.invoke(
        ls, [str(testdir / 'new_dir2' / 'new_dir')]).output
    assert not runner.invoke(ls, [str(testdir)]).output.endswith('new_dir\n')

    result_dst_path_isdir = runner.invoke(
        mv,
        ['-g',
         str(testdir / 'new_dir2' / 'new_dir' / 'newfile'),
         str(testdir)])

    assert result_dst_path_isdir.exit_code == 0
    assert '%' in result_dst_path_isdir.output
    assert 'newfile\n' in runner.invoke(ls, [str(testdir)]).output
    assert not runner.invoke(ls, [str(testdir / 'new_dir2' / 'new_dir')
                                 ]).output.endswith('newfile\n')

    result_dst_path_isdir = runner.invoke(
        mv, ['-r', '-g', str(testdir), 's3://bucket/'])

    assert result_dst_path_isdir.exit_code == 0
    assert '%' in result_dst_path_isdir.output
    assert 'newfile\n' in runner.invoke(
        ls, [f's3://bucket/{os.path.basename(str(testdir))}']).output
    assert not runner.invoke(ls, [str(testdir)]).output.endswith('newfile\n')

    result_dst_path_notdir = runner.invoke(
        mv, [
            '-g', f's3://bucket/{os.path.basename(str(testdir))}/newfile',
            str(testdir / 'newfile')
        ])

    assert result_dst_path_notdir.exit_code == 0
    assert '%' in result_dst_path_notdir.output
    assert 'newfile\n' not in runner.invoke(
        ls, [f's3://bucket/{os.path.basename(str(testdir))}']).output
    assert runner.invoke(ls, [str(testdir)]).output.endswith('newfile\n')


def test_rm(runner, testdir):
    result = runner.invoke(rm, [str(testdir / 'text')])

    assert result.exit_code == 0
    assert runner.invoke(ls, [str(testdir)]).output == ''


def test_size(runner, testdir):
    result = runner.invoke(size, [str(testdir / 'text')])

    assert result.exit_code == 0
    assert result.output == '5\n'


def test_md5sum(runner, testdir):
    result = runner.invoke(md5sum, [str(testdir / 'text')])

    assert result.exit_code == 0
    assert result.output == '5d41402abc4b2a76b9719d911017c592\n'


def test_mtime(runner, testdir):
    result = runner.invoke(mtime, [str(testdir / 'text')])

    assert result.exit_code == 0
    assert len(result.output.strip()) > 0


def test_stat(runner, testdir):
    result = runner.invoke(stat, [str(testdir / 'text')])

    assert result.exit_code == 0
    assert 'StatResult(size=5,' in result.output


def test_cp(runner, testdir):
    result = runner.invoke(
        cp,
        [str(testdir / 'text'), str(testdir / 'newfile')])

    assert result.exit_code == 0
    assert 'newfile\n' in runner.invoke(ls, [str(testdir)]).output
    assert 'text\n' in runner.invoke(ls, [str(testdir)]).output

    runner.invoke(mkdir, [str(testdir / 'new_dir')])
    result_dst_path_isdir = runner.invoke(
        cp,
        [str(testdir / 'text'), str(testdir / 'new_dir')])

    assert result_dst_path_isdir.exit_code == 0
    assert 'text' in runner.invoke(ls, [str(testdir / 'new_dir')]).output

    runner.invoke(mkdir, [str(testdir / 'new_dir2')])
    result_dst_path_isdir = runner.invoke(
        cp, ['-g', str(testdir / 'text'),
             str(testdir / 'new_dir2')])

    assert result_dst_path_isdir.exit_code == 0
    assert '%' in result_dst_path_isdir.output
    assert 'text' in runner.invoke(ls, [str(testdir / 'new_dir2')]).output

    runner.invoke(mkdir, [str(testdir / 'new_dir3')])
    result_dst_path_isdir = runner.invoke(
        cp, ['-r', str(testdir / 'new_dir'),
             str(testdir / 'new_dir3')])

    assert result_dst_path_isdir.exit_code == 0
    assert 'text' in runner.invoke(
        ls, [str(testdir / 'new_dir3' / 'new_dir')]).output

    runner.invoke(mkdir, [str(testdir / 'new_dir4')])
    result_dst_path_isdir = runner.invoke(
        cp, ['-r', '-g',
             str(testdir / 'new_dir'),
             str(testdir / 'new_dir4')])

    assert result_dst_path_isdir.exit_code == 0
    assert '%' in result_dst_path_isdir.output
    assert 'text' in runner.invoke(
        ls, [str(testdir / 'new_dir4' / 'new_dir')]).output


def test_sync(runner, testdir):
    result = runner.invoke(
        sync,
        [str(testdir / 'text'), str(testdir / 'newfile')])

    assert result.exit_code == 0
    assert 'newfile\n' in runner.invoke(ls, [str(testdir)]).output
    assert 'text\n' in runner.invoke(ls, [str(testdir)]).output

    runner.invoke(mkdir, [str(testdir / 'newdir')])
    result = runner.invoke(
        sync,
        ['-g', str(testdir / 'text'),
         str(testdir / 'newdir' / 'newfile')])

    assert result.exit_code == 0
    assert '%' in result.output
    assert 'newfile\n' in runner.invoke(ls, [str(testdir / 'newdir')]).output

    runner.invoke(mkdir, [str(testdir / 'newdir2')])
    glob_result = runner.invoke(
        sync, ['-g', str(testdir / '*'),
               str(testdir / 'newdir2')])

    assert glob_result.exit_code == 0
    assert '%' in glob_result.output
    assert 'newfile\n' in runner.invoke(ls, [str(testdir / 'newdir2')]).output
    assert 'text\n' in runner.invoke(ls, [str(testdir / 'newdir2')]).output


def test_head_and_tail(runner, tmpdir, mocker):
    with open(str(tmpdir / 'text'), 'w') as f:
        for i in range(10):
            f.write(str(i))
            f.write('\n')

    with open(str(tmpdir / 'text2'), 'w') as f:
        f.write('0')

    result = runner.invoke(head, ['-n', '2', str(tmpdir / 'text')])

    assert result.exit_code == 0
    assert result.output == '0\n1\n'

    result = runner.invoke(head, [str(tmpdir / 'text2')])

    assert result.exit_code == 0
    assert result.output == '0\n'

    result = runner.invoke(tail, ['-n', '2', str(tmpdir / 'text')])

    assert result.exit_code == 0
    assert result.output == '9\n\n'

    mocker.patch('megfile.cli.DEFAULT_BLOCK_SIZE', 1)
    result = runner.invoke(tail, ['-n', '5', str(tmpdir / 'text')])

    assert result.exit_code == 0
    assert result.output == '6\n7\n8\n9\n\n'


def test_to(runner, tmpdir):
    result = runner.invoke(to, ['-o', str(tmpdir / 'text')], b'test')
    assert result.output == 'test'

    with open(str(tmpdir / 'text'), 'rb') as f:
        assert f.read() == b'test'

    result = runner.invoke(to, ['-a', '-o', str(tmpdir / 'text')], b'test2')
    assert result.output == 'test2'

    with open(str(tmpdir / 'text'), 'rb') as f:
        assert f.read() == b'testtest2'


def test_s3(tmpdir,runner):
    result = runner.invoke(
        s3, [
            '-p', str(tmpdir / 'oss_config'), '-e', 'Endpoint', '-s', 'Addressing',
            'Aws_access_key_id', 'Aws_secret_access_key'
        ])
    assert 'Your oss config' in result.output

    result = runner.invoke(
        s3, [
            '-p', str(tmpdir / 'oss_config'), '-n', 'new_test', '-e', 'end-point',
            '-s', 'add', '1345', '2345'
        ])
    assert 'Your oss config' in result.output

    result = runner.invoke(
        s3,
        ['-p', str(tmpdir / 'oss_config'), '-n', 'new_test','7656', '3645'])
    assert 'Your oss config' in result.output
    assert 'config has been updated' in result.output

    try:
        result = runner.invoke(
            s3,
            ['-p', str(tmpdir / 'oss_config'), '-n','new_test','-c','7656', '3645'])
        assert False
    except:
        assert True

    result = runner.invoke(
        s3, ['-p', str(tmpdir / 'oss_config'), '-n', 'nothing', '7656', '3645'])
    assert 'Your oss config' in result.output
    
    with open(str(tmpdir / 'oss_config'),'r') as fp:
        text = fp.read()
        assert '[new_test]' in text
        assert '[nothing]' in text
