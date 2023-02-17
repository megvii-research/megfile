import os
import shutil
import sys
import time
from functools import wraps

import click
from tqdm import tqdm

from megfile import errors
from megfile.interfaces import FileEntry
from megfile.lib.glob import has_magic
from megfile.smart import smart_copy, smart_getmd5, smart_getmtime, smart_getsize, smart_glob, smart_glob_stat, smart_isdir, smart_isfile, smart_makedirs, smart_move, smart_open, smart_path_join, smart_remove, smart_rename, smart_scan_stat, smart_scandir, smart_stat, smart_sync, smart_touch, smart_unlink
from megfile.utils import get_human_size
from megfile.version import VERSION


@click.group()
def cli():
    """Megfile Client"""


def safe_cli():
    # try:
    cli()
    # except Exception as e:
    # click.echo(f"\n{e}")


def get_no_glob_root_path(path):
    root_dir = []
    for name in path.split('/'):
        if has_magic(name):
            break
        root_dir.append(name)
    if root_dir:
        root_dir = os.path.join(*root_dir)
    else:
        root_dir = "/" if path.startswith('/') else "."
    return root_dir


def simple_echo(file, show_full_path: bool = False):
    click.echo(file.path if show_full_path else file.name)


def long_echo(file, show_full_path: bool = False):
    click.echo(
        '%12d %s %s' % (
            file.stat.size,
            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(
                file.stat.mtime)), file.path if show_full_path else file.name))


def human_echo(file, show_full_path: bool = False):
    click.echo(
        '%10s %s %s' % (
            get_human_size(file.stat.size),
            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(
                file.stat.mtime)), file.path if show_full_path else file.name))


def smart_list_stat(path):
    if smart_isfile(path):
        yield FileEntry(os.path.basename(path), path, smart_stat(path))
    else:
        yield from smart_scandir(path)


@cli.command(short_help='List all the objects in the path.')
@click.argument('path')
@click.option(
    '-l',
    '--long',
    is_flag=True,
    help='List all the objects in the path with size, modification time and path.'
)
@click.option(
    '-r',
    '--recursive',
    is_flag=True,
    help=
    'Command is performed on all files or objects under the specified directory or prefix.'
)
@click.option(
    '-h',
    '--human-readable',
    is_flag=True,
    help='Displays file sizes in human readable format.')
def ls(path: str, long: bool, recursive: bool, human_readable: bool):
    show_full_path = False
    if has_magic(path):
        scan_func = smart_glob_stat
        show_full_path = True
    elif recursive:
        scan_func = smart_scan_stat
    else:
        scan_func = smart_list_stat
    if long:
        if human_readable:
            echo_func = human_echo
        else:
            echo_func = long_echo
    else:
        echo_func = simple_echo

    for file in scan_func(path):
        echo_func(file, show_full_path)


@cli.command(
    short_help='Copy files from source to dest, skipping already copied.')
@click.argument('src_path')
@click.argument('dst_path')
@click.option(
    '-r',
    '--recursive',
    is_flag=True,
    help=
    'Command is performed on all files or objects under the specified directory or prefix.'
)
@click.option(
    '-T',
    '--no-target-directory',
    is_flag=True,
    help='treat dst_path as a normal file.')
@click.option('-g', '--progress-bar', is_flag=True, help='Show progress bar.')
def cp(
        src_path: str,
        dst_path: str,
        recursive: bool,
        no_target_directory: bool,
        progress_bar: bool,
):
    if smart_isdir(dst_path) and not no_target_directory:
        dst_path = smart_path_join(dst_path, os.path.basename(src_path))
    copy_func = smart_sync if recursive else smart_copy
    copy_func(src_path, dst_path, show_progress=progress_bar)


@cli.command(short_help='Move files from source to dest.')
@click.argument('src_path')
@click.argument('dst_path')
@click.option(
    '-r',
    '--recursive',
    is_flag=True,
    help=
    'Command is performed on all files or objects under the specified directory or prefix.'
)
@click.option(
    '-T',
    '--no-target-directory',
    is_flag=True,
    help='treat dst_path as a normal file.')
@click.option('-g', '--progress-bar', is_flag=True, help='Show progress bar.')
def mv(
        src_path: str,
        dst_path: str,
        recursive: bool,
        no_target_directory: bool,
        progress_bar: bool,
):
    if smart_isdir(dst_path) and not no_target_directory:
        dst_path = smart_path_join(dst_path, os.path.basename(src_path))
    move_func = smart_move if recursive else smart_rename
    move_func(src_path, dst_path, show_progress=progress_bar)


@cli.command(short_help='Remove files from path.')
@click.argument('path')
@click.option(
    '-r',
    '--recursive',
    is_flag=True,
    help=
    'Command is performed on all files or objects under the specified directory or prefix.'
)
def rm(path: str, recursive: bool):
    remove_func = smart_remove if recursive else smart_unlink
    remove_func(path)


@cli.command(
    short_help='Make source and dest identical, modifying destination only.')
@click.argument('src_path')
@click.argument('dst_path')
@click.option('-g', '--progress-bar', is_flag=True, help='Show progress bar.')
def sync(src_path: str, dst_path: str, progress_bar: bool):
    if has_magic(src_path):
        root_dir = get_no_glob_root_path(src_path)

        def sync_magic_path(src_file_path):
            content_path = os.path.relpath(src_file_path, start=root_dir)
            if len(content_path) and content_path != '.':
                dst_abs_file_path = smart_path_join(
                    dst_path, content_path.lstrip('/'))
            else:
                # if content_path is empty, which means smart_isfile(src_path) is True, this function is equal to smart_copy
                dst_abs_file_path = dst_path
            smart_sync(src_file_path, dst_abs_file_path)

        if progress_bar:
            glob_paths = list(
                smart_glob(src_path, recursive=True, missing_ok=True))
            with tqdm(total=len(glob_paths)) as t:
                for src_file_path in glob_paths:
                    sync_magic_path(src_file_path)
                    t.update(1)
        else:
            for src_file_path in smart_glob(src_path, recursive=True,
                                            missing_ok=True):
                sync_magic_path(src_file_path)

    else:
        smart_sync(src_path, dst_path, show_progress=progress_bar)


@cli.command(short_help="Make the path if it doesn't already exist.")
@click.argument('path')
def mkdir(path: str):
    smart_makedirs(path)


@cli.command(short_help="Make the file if it doesn't already exist.")
@click.argument('path')
def touch(path: str):
    smart_touch(path)


@cli.command(short_help='Concatenate any files and send them to stdout.')
@click.argument('path')
def cat(path: str):
    with smart_open(path, 'rb') as file:
        shutil.copyfileobj(file, sys.stdout.buffer)


@cli.command(
    short_help='Produce an md5sum file for all the objects in the path.')
@click.argument('path')
def md5sum(path: str):
    click.echo(smart_getmd5(path))


@cli.command(
    short_help='Return the total size and number of objects in remote:path.')
@click.argument('path')
def size(path: str):
    click.echo(smart_getsize(path))


@cli.command(
    short_help='Return the mtime and number of objects in remote:path.')
@click.argument('path')
def mtime(path: str):
    click.echo(smart_getmtime(path))


@cli.command(short_help='Return the stat and number of objects in remote:path.')
@click.argument('path')
def stat(path: str):
    click.echo(smart_stat(path))


@cli.command(short_help='Return the megfile version.')
def version():
    click.echo(VERSION)


if __name__ == '__main__':
    # Usage: python -m megfile.cli
    cli()  # pragma: no cover
