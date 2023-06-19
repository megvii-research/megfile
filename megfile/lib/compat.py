import os
from os import PathLike

__all__ = [
    'PathLike',
    'fspath',
    'copytree',
]


def fspath(path) -> str:
    result = os.fspath(path)
    if isinstance(result, bytes):
        return result.decode()
    return result


import sys

if sys.version_info.major == 3 and sys.version_info.minor >= 8:
    from shutil import copytree  # pragma: no cover
else:
    from shutil import Error, copy2, copystat  # pragma: no cover

    def copytree(
            src,
            dst,
            symlinks=False,
            ignore=None,
            copy_function=copy2,
            ignore_dangling_symlinks=False,
            dirs_exist_ok=False):  # pragma: no cover
        names = os.listdir(src)
        if ignore is not None:
            ignored_names = ignore(src, names)
        else:
            ignored_names = set()

        os.makedirs(dst, exist_ok=dirs_exist_ok)
        errors = []
        for name in names:
            if name in ignored_names:
                continue
            srcname = os.path.join(src, name)
            dstname = os.path.join(dst, name)
            try:
                if os.path.islink(srcname):
                    linkto = os.readlink(srcname)
                    if symlinks:
                        # We can't just leave it to `copy_function` because legacy
                        # code with a custom `copy_function` may rely on copytree
                        # doing the right thing.
                        os.symlink(linkto, dstname)
                        copystat(srcname, dstname, follow_symlinks=not symlinks)
                    else:
                        # ignore dangling symlink if the flag is on
                        if not os.path.exists(
                                linkto) and ignore_dangling_symlinks:
                            continue
                        # otherwise let the copy occurs. copy2 will raise an error
                        if os.path.isdir(srcname):
                            copytree(
                                srcname, dstname, symlinks, ignore,
                                copy_function)
                        else:
                            copy_function(srcname, dstname)
                elif os.path.isdir(srcname):
                    copytree(srcname, dstname, symlinks, ignore, copy_function)
                else:
                    # Will raise a SpecialFileError for unsupported file types
                    copy_function(srcname, dstname)
            # catch the Error from the recursive copytree so that we can
            # continue with other files
            except Error as err:
                errors.extend(err.args[0])
            except OSError as why:
                errors.append((srcname, dstname, str(why)))
        try:
            copystat(src, dst)
        except OSError as why:
            # Copying file access times may fail on Windows
            if getattr(why, 'winerror', None) is None:
                errors.append((src, dst, str(why)))
        if errors:
            raise Error(errors)
        return dst
