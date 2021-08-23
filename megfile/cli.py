import os
import time

import click

from megfile.interfaces import FileEntry
from megfile.smart import smart_copy, smart_getmd5, smart_getmtime, smart_getsize, smart_isfile, smart_makedirs, smart_move, smart_open, smart_remove, smart_rename, smart_scan_stat, smart_scandir, smart_stat, smart_sync, smart_touch, smart_unlink
from megfile.utils import get_human_size
from megfile.version import VERSION


@click.group()
def cli():
    pass


def simple_echo(file):
    click.echo(file.name)


def long_echo(file):
    click.echo(
        '%12d %s %s' % (
            file.stat.size,
            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(
                file.stat.mtime)), file.name))


def human_echo(file):
    click.echo(
        '%10s %s %s' % (
            get_human_size(file.stat.size),
            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(
                file.stat.mtime)), file.name))


def smart_list_stat(path):
    if smart_isfile(path):
        yield FileEntry(os.path.basename(path), smart_stat(path))
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
    scan_func = smart_scan_stat if recursive else smart_list_stat
    if long:
        if human_readable:
            echo_func = human_echo
        else:
            echo_func = long_echo
    else:
        echo_func = simple_echo

    for file in scan_func(path):
        echo_func(file)


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
def cp(src_path: str, dst_path: str, recursive: bool):
    copy_func = smart_sync if recursive else smart_copy
    copy_func(src_path, dst_path)


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
def mv(src_path: str, dst_path: str, recursive: bool):
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
def sync(src_path: str, dst_path: str):
    smart_sync(src_path, dst_path)


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
    with smart_open(path, 'r') as f:
        for line in f:
            click.echo(line)


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
