# megfile.smart_path module

### *class* megfile.smart_path.SmartPath(path: str | BasePath | PathLike | int, \*other_paths: str | BasePath | PathLike)

Bases: `BasePath`

#### absolute(\*args, \*\*kwargs)

Dynamically bound method for absolute

#### abspath(\*args, \*\*kwargs)

Dynamically bound method for abspath

#### access(\*args, \*\*kwargs)

Dynamically bound method for access

#### *property* anchor

#### as_posix(\*args, \*\*kwargs)

Dynamically bound method for as_posix

#### as_uri(\*args, \*\*kwargs)

Dynamically bound method for as_uri

#### chmod(\*args, \*\*kwargs)

Dynamically bound method for chmod

#### cwd(\*args, \*\*kwargs)

Dynamically bound method for cwd

#### *property* drive

#### exists(\*args, \*\*kwargs)

Dynamically bound method for exists

#### expanduser(\*args, \*\*kwargs)

Dynamically bound method for expanduser

#### *classmethod* from_uri(path: str | BasePath | PathLike)

#### getmtime(\*args, \*\*kwargs)

Dynamically bound method for getmtime

#### getsize(\*args, \*\*kwargs)

Dynamically bound method for getsize

#### glob(\*args, \*\*kwargs)

Dynamically bound method for glob

#### glob_stat(\*args, \*\*kwargs)

Dynamically bound method for glob_stat

#### group(\*args, \*\*kwargs)

Dynamically bound method for group

#### hardlink_to(\*args, \*\*kwargs)

Dynamically bound method for hardlink_to

#### home(\*args, \*\*kwargs)

Dynamically bound method for home

#### iglob(\*args, \*\*kwargs)

Dynamically bound method for iglob

#### is_absolute(\*args, \*\*kwargs)

Dynamically bound method for is_absolute

#### is_block_device(\*args, \*\*kwargs)

Dynamically bound method for is_block_device

#### is_char_device(\*args, \*\*kwargs)

Dynamically bound method for is_char_device

#### is_dir(\*args, \*\*kwargs)

Dynamically bound method for is_dir

#### is_fifo(\*args, \*\*kwargs)

Dynamically bound method for is_fifo

#### is_file(\*args, \*\*kwargs)

Dynamically bound method for is_file

#### is_mount(\*args, \*\*kwargs)

Dynamically bound method for is_mount

#### is_relative_to(\*args, \*\*kwargs)

Dynamically bound method for is_relative_to

#### is_reserved(\*args, \*\*kwargs)

Dynamically bound method for is_reserved

#### is_socket(\*args, \*\*kwargs)

Dynamically bound method for is_socket

#### is_symlink(\*args, \*\*kwargs)

Dynamically bound method for is_symlink

#### iterdir(\*args, \*\*kwargs)

Dynamically bound method for iterdir

#### joinpath(\*args, \*\*kwargs)

Dynamically bound method for joinpath

#### lchmod(\*args, \*\*kwargs)

Dynamically bound method for lchmod

#### listdir(\*args, \*\*kwargs)

Dynamically bound method for listdir

#### load(\*args, \*\*kwargs)

Dynamically bound method for load

#### lstat(\*args, \*\*kwargs)

Dynamically bound method for lstat

#### match(\*args, \*\*kwargs)

Dynamically bound method for match

#### md5(\*args, \*\*kwargs)

Dynamically bound method for md5

#### mkdir(\*args, \*\*kwargs)

Dynamically bound method for mkdir

#### *property* name

A string representing the final path component, excluding the drive and root

#### open(\*args, \*\*kwargs)

Dynamically bound method for open

#### owner(\*args, \*\*kwargs)

Dynamically bound method for owner

#### *property* parent

The logical parent of the path

#### *property* parents *: URIPathParents*

An immutable sequence providing access to the logical ancestors of the path

#### *property* parts *: Tuple[str, ...]*

A tuple giving access to the path’s various components

#### read_bytes(\*args, \*\*kwargs)

Dynamically bound method for read_bytes

#### read_text(\*args, \*\*kwargs)

Dynamically bound method for read_text

#### readlink(\*args, \*\*kwargs)

Dynamically bound method for readlink

#### realpath(\*args, \*\*kwargs)

Dynamically bound method for realpath

#### *classmethod* register(path_class, override_ok: bool = False)

#### relative_to(\*args, \*\*kwargs)

Dynamically bound method for relative_to

#### relpath(start: str | None = None) → str

Return the relative path of given path

* **Parameters:**
  **start** – Given start directory
* **Returns:**
  Relative path from start

#### remove(\*args, \*\*kwargs)

Dynamically bound method for remove

#### rename(\*args, \*\*kwargs)

Dynamically bound method for rename

#### replace(\*args, \*\*kwargs)

Dynamically bound method for replace

#### resolve(\*args, \*\*kwargs)

Dynamically bound method for resolve

#### rglob(\*args, \*\*kwargs)

Dynamically bound method for rglob

#### rmdir(\*args, \*\*kwargs)

Dynamically bound method for rmdir

#### *property* root

#### samefile(\*args, \*\*kwargs)

Dynamically bound method for samefile

#### save(\*args, \*\*kwargs)

Dynamically bound method for save

#### scan(\*args, \*\*kwargs)

Dynamically bound method for scan

#### scan_stat(\*args, \*\*kwargs)

Dynamically bound method for scan_stat

#### scandir(\*args, \*\*kwargs)

Dynamically bound method for scandir

#### stat(\*args, \*\*kwargs)

Dynamically bound method for stat

#### *property* stem

The final path component, without its suffix

#### *property* suffix

The file extension of the final component

#### *property* suffixes

A list of the path’s file extensions

#### symlink(\*args, \*\*kwargs)

Dynamically bound method for symlink

#### symlink_to(\*args, \*\*kwargs)

Dynamically bound method for symlink_to

#### touch(\*args, \*\*kwargs)

Dynamically bound method for touch

#### unlink(\*args, \*\*kwargs)

Dynamically bound method for unlink

#### utime(\*args, \*\*kwargs)

Dynamically bound method for utime

#### walk(\*args, \*\*kwargs)

Dynamically bound method for walk

#### with_name(\*args, \*\*kwargs)

Dynamically bound method for with_name

#### with_stem(\*args, \*\*kwargs)

Dynamically bound method for with_stem

#### with_suffix(\*args, \*\*kwargs)

Dynamically bound method for with_suffix

#### write_bytes(\*args, \*\*kwargs)

Dynamically bound method for write_bytes

#### write_text(\*args, \*\*kwargs)

Dynamically bound method for write_text

### megfile.smart_path.get_traditional_path(path: str | BasePath | PathLike) → str
