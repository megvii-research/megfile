import os

import pytest
from click.testing import CliRunner

from megfile.cli import cat, cp, get_no_glob_root_path, ls, md5sum, mkdir, mtime, mv, rm, size, stat, sync, touch, version

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

    glob_result_file = runner.invoke(ls, [str(testdir / "*")])
    assert glob_result_file.exit_code == 0
    assert glob_result_file.output == "%s/%s\n" % (testdir, file_name)


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
    assert '100%' in result_dst_path_isdir.output
    assert 'newfile\n' in runner.invoke(
        ls, [str(testdir / 'new_dir2' / 'new_dir')]).output
    assert not runner.invoke(ls, [str(testdir)]).output.endswith('new_dir\n')

    result_dst_path_isdir = runner.invoke(
        mv,
        ['-g',
         str(testdir / 'new_dir2' / 'new_dir' / 'newfile'),
         str(testdir)])

    assert result_dst_path_isdir.exit_code == 0
    assert '100%' in result_dst_path_isdir.output
    assert 'newfile\n' in runner.invoke(ls, [str(testdir)]).output
    assert not runner.invoke(ls, [str(testdir / 'new_dir2' / 'new_dir')
                                 ]).output.endswith('newfile\n')

    result_dst_path_isdir = runner.invoke(
        mv, ['-r', '-g', str(testdir), 's3://bucket/'])

    assert result_dst_path_isdir.exit_code == 0
    assert '100%' in result_dst_path_isdir.output
    assert 'newfile\n' in runner.invoke(
        ls, [f's3://bucket/{os.path.basename(str(testdir))}']).output
    assert not runner.invoke(ls, [str(testdir)]).output.endswith('newfile\n')


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
    assert '100%' in result_dst_path_isdir.output
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
    assert '100%' in result_dst_path_isdir.output
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
    assert '100%' in result.output
    assert 'newfile\n' in runner.invoke(ls, [str(testdir / 'newdir')]).output

    runner.invoke(mkdir, [str(testdir / 'newdir2')])
    glob_result = runner.invoke(
        sync, ['-g', str(testdir / '*'),
               str(testdir / 'newdir2')])

    assert glob_result.exit_code == 0
    assert '100%' in glob_result.output
    assert 'newfile\n' in runner.invoke(ls, [str(testdir / 'newdir2')]).output
    assert 'text\n' in runner.invoke(ls, [str(testdir / 'newdir2')]).output


def test_get_no_glob_root_path():
    assert get_no_glob_root_path('/data/**/*.py') == '/data'
    assert get_no_glob_root_path('/**/*.py') == '/'
    assert get_no_glob_root_path('./**/*.py') == '.'
    assert get_no_glob_root_path('**/*.py') == '.'
