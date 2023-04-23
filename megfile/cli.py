import logging
import os
import shutil
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import click
from tqdm import tqdm

from megfile.interfaces import FileEntry
from megfile.lib.glob import get_non_glob_dir, has_magic
from megfile.smart import smart_copy, smart_getmd5, smart_getmtime, smart_getsize, smart_glob, smart_glob_stat, smart_isdir, smart_isfile, smart_makedirs, smart_move, smart_open, smart_path_join, smart_remove, smart_rename, smart_scan_stat, smart_scandir, smart_stat, smart_sync, smart_sync_with_progress, smart_touch, smart_unlink
from megfile.smart_path import SmartPath
from megfile.utils import get_human_size
from megfile.version import VERSION

logging.basicConfig(level=logging.INFO)


@click.group()
def cli():
    """Megfile Client"""


def safe_cli():  # pragma: no cover
    try:
        cli()
    except Exception as e:
        click.echo(f"\n[{type(e).__name__}] {e}", err=True)


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
    if recursive:
        with ThreadPoolExecutor(max_workers=(os.cpu_count() or 1) *
                                2) as executor:
            if progress_bar:
                smart_sync_with_progress(
                    src_path, dst_path, followlinks=True, map_func=executor.map)
            else:
                smart_sync(
                    src_path, dst_path, followlinks=True, map_func=executor.map)
    else:
        if progress_bar:
            file_size = smart_stat(src_path).size
            sbar = tqdm(total=file_size, unit='B', ascii=True, unit_scale=True)

            def callback(length: int):
                sbar.update(length)

            smart_copy(src_path, dst_path, callback=callback)
            sbar.close()
        else:
            smart_copy(src_path, dst_path)


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
    if progress_bar:
        src_protocol, _ = SmartPath._extract_protocol(src_path)
        dst_protocol, _ = SmartPath._extract_protocol(dst_path)

        if recursive:
            if src_protocol == dst_protocol:
                with tqdm(total=1) as t:
                    SmartPath(src_path).rename(dst_path)
                    t.update(1)
            else:
                smart_sync_with_progress(src_path, dst_path, followlinks=True)
                smart_remove(src_path)
        else:
            if src_protocol == dst_protocol:
                with tqdm(total=1) as t:
                    SmartPath(src_path).rename(dst_path)
                    t.update(1)
            else:
                file_size = smart_stat(src_path).size
                sbar = tqdm(
                    total=file_size, unit='B', ascii=True, unit_scale=True)

                def callback(length: int):
                    sbar.update(length)

                smart_copy(src_path, dst_path, callback=callback)
                smart_unlink(src_path)
                sbar.close()
    else:
        move_func = smart_move if recursive else smart_rename
        move_func(src_path, dst_path)


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
    with ThreadPoolExecutor(max_workers=(os.cpu_count() or 1) * 2) as executor:
        if has_magic(src_path):
            root_dir = get_non_glob_dir(src_path)
            path_stats = []
            for dir_or_file in smart_glob(src_path, recursive=True,
                                          missing_ok=True):
                path_stats.extend(list(smart_scan_stat(dir_or_file)))

            if progress_bar:
                tbar = tqdm(total=len(path_stats), ascii=True)
                sbar = tqdm(unit='B', ascii=True, unit_scale=True)

                def callback(_filename: str, length: int):
                    sbar.update(length)

                def callback_after_copy_file(src_file_path, dst_file_path):
                    tbar.update(1)

                smart_sync(
                    root_dir,
                    dst_path,
                    callback=callback,
                    callback_after_copy_file=callback_after_copy_file,
                    src_file_stats=path_stats,
                    map_func=executor.map,
                )

                tbar.close()
                sbar.close()
            else:  # pragma: no cover
                smart_sync(
                    root_dir,
                    dst_path,
                    src_file_stats=path_stats,
                    map_func=executor.map,
                )
        else:
            if progress_bar:
                smart_sync_with_progress(src_path, dst_path, followlinks=True)
            else:
                smart_sync(
                    src_path, dst_path, followlinks=True, map_func=executor.map)


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
    safe_cli()  # pragma: no cover
