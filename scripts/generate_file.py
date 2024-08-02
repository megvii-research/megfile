import importlib
import re

ALL_IGNORE_FUNC_LIST = dict(
    s3=[
        "open",
        "readlink",
        "iterdir",
        "is_mount",
        "is_socket",
        "is_fifo",
        "is_block_device",
        "is_char_device",
        "owner",
        "absolute",
        "rmdir",
        "glob",
        "iglob",
        "glob_stat",
        "rename",
        "cwd",
        "mkdir",
        "parts",
        "path_without_protocol",
        "path_with_protocol",
    ],
    fs=[
        "open",
        "from_uri",
        "path_with_protocol",
        "joinpath",
        "readlink",
        "iterdir",
        "chmod",
        "group",
        "is_socket",
        "is_fifo",
        "is_block_device",
        "is_char_device",
        "rmdir",
        "owner",
        "absolute",
        "resolve",
        "cwd",
        "home",
        "glob",
        "iglob",
        "glob_stat",
        "rename",
        "parts",
        "root",
        "anchor",
        "drive",
        "replace",
        "hardlink_to",
        "mkdir",
        "utime",
    ],
    http=["open"],
    sftp=[
        "path_without_protocol",
        "expanduser",
        "iterdir",
        "readlink",
        "cwd",
        "glob",
        "iglob",
        "glob_stat",
        "resolve",
        "relpath",
        "utime",
        "parts",
    ],
    hdfs=[
        "iterdir",
        "absolute",
        "rmdir",
        "glob",
        "iglob",
        "glob_stat",
        "rename",
        "mkdir",
        "path_without_protocol",
        "path_with_protocol",
        "parts",
    ],
)

ALL_IMPORT_LINES = dict(
    s3=[
        "from typing import BinaryIO, Callable, Iterator, List, Optional, Tuple",
        "from megfile.interfaces import Access, FileEntry, PathLike, StatResult",
    ],
    fs=[
        "from typing import BinaryIO, Callable, Iterator, List, Optional, Tuple",
        "from megfile.interfaces import Access, FileEntry, PathLike, StatResult",
    ],
    http=["from megfile.interfaces import PathLike, StatResult"],
    stdio=[
        "from typing import IO, AnyStr, Optional",
        "from megfile.interfaces import PathLike",
    ],
    sftp=[
        "from typing import IO, AnyStr, BinaryIO, Iterator, List, Tuple, "
        "Callable, Optional",
        "from megfile.interfaces import FileEntry, PathLike, StatResult",
    ],
    hdfs=[
        "from typing import IO, AnyStr, BinaryIO, Iterator, List, Optional, Tuple",
        "from megfile.interfaces import FileEntry, PathLike, StatResult",
    ],
)

ALL_FUNC_NAME_MAPPING = dict(
    s3=dict(
        is_dir="isdir",
        is_file="isfile",
        load="load_from",
        mkdir="makedirs",
        md5="getmd5",
        symlink_to="symlink",
        is_symlink="islink",
        save="save_as",
    ),
    fs=dict(
        is_dir="isdir",
        is_file="isfile",
        md5="getmd5",
        load="load_from",
        mkdir="makedirs",
        symlink_to="symlink",
        is_symlink="islink",
        is_mount="ismount",
        save="save_as",
        joinpath="path_join",
        is_absolute="isabs",
        replace="move",
    ),
    http=dict(),
    sftp=dict(
        is_dir="isdir",
        is_file="isfile",
        md5="getmd5",
        load="load_from",
        mkdir="makedirs",
        symlink_to="symlink",
        is_symlink="islink",
        save="save_as",
        is_absolute="isabs",
        replace="move",
    ),
    hdfs=dict(
        is_dir="isdir",
        is_file="isfile",
        load="load_from",
        mkdir="makedirs",
        md5="getmd5",
        symlink_to="symlink",
        is_symlink="islink",
        save="save_as",
    ),
)
PARAMETER_PATTERN = re.compile(r"\[[^:]*\]")


def get_class_name(current_file_type: str):
    if current_file_type == "fs":
        return "FSPath"
    return f"{current_file_type.capitalize()}Path"


def insert_class_method_lines(
    func_params: list, annotation_lines: list, current_file_type: str
):
    ignore_func_list = ALL_IGNORE_FUNC_LIST.get(current_file_type, [])
    func_name_mapping = ALL_FUNC_NAME_MAPPING.get(current_file_type, {})

    real_func_name, func_content_lines = None, []
    if func_params:
        func_first_line = "".join(func_params)
        path_param_name = "path"
        current_params_line = PARAMETER_PATTERN.sub(
            "", func_first_line.split("(", maxsplit=1)[1].split(")", maxsplit=1)[0]
        )
        current_params = []
        kwargs_mode = False
        for params_words in current_params_line.split(","):
            if ":" in params_words:
                param = params_words.split(":", maxsplit=1)[0].strip()
            elif "=" in params_words:
                param = params_words.split("=", maxsplit=1)[0].strip()
            else:
                param = params_words.strip()
            if param == "*":
                kwargs_mode = True
                continue

            if param and param != "**kwargs":
                if "dst" in param:
                    path_param_name = param.replace("dst", "src")
                if kwargs_mode:
                    param = f"{param}={param}"
                current_params.append(param)
        func_name = (
            func_first_line.strip()
            .split("def ", maxsplit=1)[1]
            .split("(", maxsplit=1)[0]
        )
        if func_name == "save":
            func_first_line = func_first_line.replace(
                "self", f"{path_param_name}: PathLike"
            )
            special_order_params = [
                param.strip()
                for param in func_first_line.split("(", 1)[1]
                .split(")", 1)[0]
                .split(",")
            ]
            special_order_params[0], special_order_params[1] = (
                special_order_params[1],
                special_order_params[0],
            )
            func_first_line = "".join(
                [
                    func_first_line.split("(", 1)[0],
                    "(",
                    ", ".join(special_order_params),
                    ")",
                    func_first_line.split(")", 1)[1],
                ]
            )
        else:
            func_first_line = func_first_line.replace(
                "self", f"{path_param_name}: PathLike"
            )

        if not func_name.startswith("_") and func_name not in ignore_func_list:
            real_func_name = (
                f"{current_file_type}_{func_name_mapping.get(func_name, func_name)}"
            )
            func_content_lines.append(
                func_first_line.replace(func_name, real_func_name).replace(
                    ", **kwargs", ""
                )
            )

            insert_log = False
            for annotation_line in annotation_lines:
                if insert_log is False and annotation_line.strip().startswith(":"):
                    func_content_lines.append(
                        f"    :param {path_param_name}: Given path"
                    )
                    insert_log = True
                func_content_lines.append(annotation_line)

            class_name = get_class_name(current_file_type)
            if class_name == "StdioPath":
                func_content_lines.append(
                    f"    return {class_name}({path_param_name}).{func_name}({', '.join(current_params[1:])})  # pyre-ignore[6]\n\n"  # noqa: E501
                )
            else:
                func_content_lines.append(
                    f"    return {class_name}({path_param_name}).{func_name}({', '.join(current_params[1:])})\n\n"  # noqa: E501
                )
    return real_func_name, func_content_lines


def get_methods_from_path_file(current_file_type: str):
    all_func_list = importlib.import_module(f"megfile.{current_file_type}_path").__all__
    methods_content = []
    import_lines = ALL_IMPORT_LINES.get(current_file_type, [])
    import_lines.append(
        f"from megfile.{current_file_type}_path import {', '.join(all_func_list)}"
    )
    with open(f"megfile/{current_file_type}_path.py", "r") as f:
        class_start = False
        func_start = False
        func_params = []
        annotation_lines = []
        annotation_start = False
        for line in f.readlines():
            if line.strip().startswith(f"class {get_class_name(current_file_type)}("):
                class_start = True
            elif class_start is True:
                if line.strip() and not line.startswith(" " * 4):
                    break
                elif func_start is True:
                    if line.rsplit("#", maxsplit=1)[0].strip().endswith(":"):
                        func_start = False
                    func_params.append(line.strip())
                elif "'''" in line or '"""' in line:
                    if line.count("'''") <= 1 and line.count('"""') <= 1:
                        annotation_start = not annotation_start
                    annotation_lines.append(line[4:].rstrip())
                elif annotation_start is True:
                    annotation_lines.append(line[4:].rstrip())
                elif line.startswith("    def"):
                    if line.rsplit("#", maxsplit=1)[0].strip().endswith(":"):
                        func_start = False
                    else:
                        func_start = True
                    func_name, func_content_lines = insert_class_method_lines(
                        func_params, annotation_lines, current_file_type
                    )
                    if func_name:
                        all_func_list.append(func_name)
                    if func_content_lines:
                        methods_content.extend(func_content_lines)
                    func_params = [line.strip()]
                    annotation_lines = []
        func_name, func_content_lines = insert_class_method_lines(
            func_params, annotation_lines, current_file_type
        )
        if func_name:
            all_func_list.append(func_name)
        if func_content_lines:
            methods_content.extend(func_content_lines)
        return import_lines, all_func_list, methods_content


def generate_file(current_file_type: str):
    current_class_name = get_class_name(current_file_type)
    import_lines, all_func_list, methods_content = get_methods_from_path_file(
        current_file_type
    )
    with open(f"megfile/{current_file_type}.py", "w") as f:
        for line in import_lines:
            f.write("\n")
            f.write(line)

        f.write("\n\n__all__ = [\n")
        for func_name in all_func_list:
            if func_name != current_class_name and func_name != "HttpsPath":
                f.write(f"    '{func_name}',\n")
        f.write("]\n\n")

        for line in methods_content:
            f.write("\n")
            f.write(line)


if __name__ == "__main__":
    for t in ["s3", "fs", "http", "stdio", "sftp", "hdfs"]:
        generate_file(t)
