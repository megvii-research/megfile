import os
from collections import namedtuple

import pytest

from megfile.lib import fnmatch, glob

File = namedtuple("File", ["path", "body"])
"""
bucketA/
|-folderAA/
  |-folderAAA/
    |-fileAAAA
|-folderAB-C/
  |-fileAB-C
|-folderAB/
  |-fileAB
  |-fileAC
|-fileAA
|-fileAB
bucketB/
bucketC/
|-folder    （目录）
    |-file

bucketForGlobTest/ （用于 glob 的测试, 结构较复杂）
|-1
    |-a （目录）
        |-b
            |-c
                |-1.json
                |-A.msg
        |-1.json
|-2
    |-a
        |-d
            |-c
                |-1.json
            |-2.json
        |-b
            |-c
                |-1.json
                |-2.json
            |-a
                |-1.json
emptyBucketForGlobTest/
"""

FILE_LIST = [
    File("/bucketA/folderAA/folderAAA/fileAAAA", "fileAAAA"),
    File("/bucketA/folderAB-C/fileAB-C", "fileAB-C"),
    File("/bucketA/folderAB/fileAB", "fileAB"),
    File("/bucketA/folderAB/fileAC", "fileAC"),
    File("/bucketA/fileAA", "fileAA"),
    File("/bucketA/fileAB", "fileAB"),
    File("/bucketB", None),  # 空 bucket
    File("/bucketC/folder/file", "file"),
    File("/bucketForGlobTest/1/a/b/c/1.json", "1.json"),
    File("/bucketForGlobTest/1/a/b/1.json", "1.json"),  # for glob(*/a/*/*.json)
    File("/bucketForGlobTest/1/a/b/c/A.msg", "A.msg"),
    File("/bucketForGlobTest/2/a/d/c/1.json", "1.json"),
    File("/bucketForGlobTest/2/a/d/2.json", "2.json"),
    File("/bucketForGlobTest/2/a/b/c/1.json", "1.json"),
    File("/bucketForGlobTest/2/a/b/c/2.json", "2.json"),
    File("/bucketForGlobTest/2/a/b/a/1.json", "1.json"),
    File("/emptyBucketForGlobTest", None),
    File("/1.json", "1.json"),
]


@pytest.fixture
def fs_setup(fs, file_list=FILE_LIST):
    for file in file_list:
        if file.body is None:
            os.makedirs(file.path, exist_ok=True)
        else:
            dirname = os.path.dirname(file.path)
            os.makedirs(dirname, exist_ok=True)
            with open(file.path, "w") as writer:
                writer.write(file.body)


def assert_glob(pattern, expected, recursive=True):
    assert sorted(glob.glob(pattern, recursive=recursive)) == sorted(expected)


def _glob_with_common_wildcard():
    """
    scenario: common shell wildcard, '*', '**', '[]', '?'
    expectation: return matched pathnames in lexicographical order
    """
    # without any wildcards
    assert_glob("/emptyBucketForGlobTest", ["/emptyBucketForGlobTest"])
    assert_glob(
        "*",
        [
            "tmp",
            "bucketA",
            "bucketB",
            "bucketC",
            "bucketForGlobTest",
            "emptyBucketForGlobTest",
            "1.json",
        ],
        recursive=False,
    )
    assert_glob("/emptyBucketForGlobTest/", ["/emptyBucketForGlobTest/"])
    assert_glob("/bucketForGlobTest/1", ["/bucketForGlobTest/1"])
    assert_glob("/bucketForGlobTest/1/", ["/bucketForGlobTest/1/"])
    assert_glob("/bucketForGlobTest/1/a", ["/bucketForGlobTest/1/a"])
    assert_glob("/bucketForGlobTest/2/a/d/2.json", ["/bucketForGlobTest/2/a/d/2.json"])

    # '*', all files and folders
    assert_glob("/emptyBucketForGlobTest/*", [])
    assert_glob(
        "/bucketForGlobTest/*", ["/bucketForGlobTest/1", "/bucketForGlobTest/2"]
    )

    # all files under all direct subfolders
    assert_glob(
        "/bucketForGlobTest/*/*", ["/bucketForGlobTest/1/a", "/bucketForGlobTest/2/a"]
    )

    # combination of '?' and []
    assert_glob("/bucketForGlobTest/[2-3]/**/*?msg", [])
    assert_glob(
        "/bucketForGlobTest/[13]/**/*?msg", ["/bucketForGlobTest/1/a/b/c/A.msg"]
    )


def _glob_with_recursive_pathname():
    """
    scenario: recursively search target folder
    expectation: returns all subdirectory and files,
    without check of lexicographical order
    """
    # recursive all files and folders
    assert_glob(
        "/bucketForGlobTest/**",
        [
            "/bucketForGlobTest/",
            "/bucketForGlobTest/1",
            "/bucketForGlobTest/1/a",
            "/bucketForGlobTest/1/a/b",
            "/bucketForGlobTest/1/a/b/1.json",
            "/bucketForGlobTest/1/a/b/c",
            "/bucketForGlobTest/1/a/b/c/1.json",
            "/bucketForGlobTest/1/a/b/c/A.msg",
            "/bucketForGlobTest/2",
            "/bucketForGlobTest/2/a",
            "/bucketForGlobTest/2/a/b",
            "/bucketForGlobTest/2/a/b/a",
            "/bucketForGlobTest/2/a/b/a/1.json",
            "/bucketForGlobTest/2/a/b/c",
            "/bucketForGlobTest/2/a/b/c/1.json",
            "/bucketForGlobTest/2/a/b/c/2.json",
            "/bucketForGlobTest/2/a/d",
            "/bucketForGlobTest/2/a/d/2.json",
            "/bucketForGlobTest/2/a/d/c/1.json",
            "/bucketForGlobTest/2/a/d/c",
        ],
    )

    assert_glob(
        "/bucketForGlobTest/**/*",
        [
            "/bucketForGlobTest/1",
            "/bucketForGlobTest/1/a",
            "/bucketForGlobTest/1/a/b",
            "/bucketForGlobTest/1/a/b/1.json",
            "/bucketForGlobTest/1/a/b/c",
            "/bucketForGlobTest/1/a/b/c/1.json",
            "/bucketForGlobTest/1/a/b/c/A.msg",
            "/bucketForGlobTest/2",
            "/bucketForGlobTest/2/a",
            "/bucketForGlobTest/2/a/b",
            "/bucketForGlobTest/2/a/b/a",
            "/bucketForGlobTest/2/a/b/a/1.json",
            "/bucketForGlobTest/2/a/b/c",
            "/bucketForGlobTest/2/a/b/c/1.json",
            "/bucketForGlobTest/2/a/b/c/2.json",
            "/bucketForGlobTest/2/a/d",
            "/bucketForGlobTest/2/a/d/2.json",
            "/bucketForGlobTest/2/a/d/c/1.json",
            "/bucketForGlobTest/2/a/d/c",
        ],
    )

    assert_glob(
        "**",
        [
            "1.json",
            "tmp",
            "bucketA",
            "bucketA/folderAA",
            "bucketA/folderAA/folderAAA",
            "bucketA/folderAA/folderAAA/fileAAAA",
            "bucketA/folderAB-C",
            "bucketA/folderAB-C/fileAB-C",
            "bucketA/folderAB",
            "bucketA/folderAB/fileAB",
            "bucketA/folderAB/fileAC",
            "bucketA/fileAA",
            "bucketA/fileAB",
            "bucketB",
            "bucketC",
            "bucketC/folder",
            "bucketC/folder/file",
            "bucketForGlobTest",
            "bucketForGlobTest/1",
            "bucketForGlobTest/1/a",
            "bucketForGlobTest/1/a/b",
            "bucketForGlobTest/1/a/b/c",
            "bucketForGlobTest/1/a/b/c/1.json",
            "bucketForGlobTest/1/a/b/c/A.msg",
            "bucketForGlobTest/1/a/b/1.json",
            "bucketForGlobTest/2",
            "bucketForGlobTest/2/a",
            "bucketForGlobTest/2/a/d",
            "bucketForGlobTest/2/a/d/c",
            "bucketForGlobTest/2/a/d/c/1.json",
            "bucketForGlobTest/2/a/d/2.json",
            "bucketForGlobTest/2/a/b",
            "bucketForGlobTest/2/a/b/c",
            "bucketForGlobTest/2/a/b/c/1.json",
            "bucketForGlobTest/2/a/b/c/2.json",
            "bucketForGlobTest/2/a/b/a",
            "bucketForGlobTest/2/a/b/a/1.json",
            "emptyBucketForGlobTest",
        ],
    )


def _glob_with_same_file_and_folder():
    """
    scenario: existing same-named file and directory in a  directory
    expectation: the file and directory is returned 1 time respectively
    """
    # same name and folder
    assert_glob(
        "/bucketForGlobTest/1/*",
        [
            # 1 file name 'a' and 1 actual folder
            "/bucketForGlobTest/1/a"
        ],
    )


def _glob_with_nested_pathname():
    """
    scenario: pathname including nested '**'
    expectation: work correctly as standard glob module
    """
    # nested
    # non-recursive, actually: /bucketForGlobTest/*/a/*/*.jso?
    assert_glob(
        "/bucketForGlobTest/**/a/**/*.jso?",
        ["/bucketForGlobTest/2/a/d/2.json", "/bucketForGlobTest/1/a/b/1.json"],
        recursive=False,
    )

    # recursive
    # /bucketForGlobTest/2/a/b/a/1.json is returned 2 times
    # without set, otherwise, '/bucketForGlobTest/2/a/b/a/1.json' would be duplicated
    assert_glob(
        "/bucketForGlobTest/**/a/**/*.jso?",
        [
            "/bucketForGlobTest/1/a/b/1.json",
            "/bucketForGlobTest/1/a/b/c/1.json",
            "/bucketForGlobTest/2/a/b/a/1.json",  # first time
            "/bucketForGlobTest/2/a/b/a/1.json",  # second time
            "/bucketForGlobTest/2/a/b/c/1.json",
            "/bucketForGlobTest/2/a/b/c/2.json",
            "/bucketForGlobTest/2/a/d/2.json",
            "/bucketForGlobTest/2/a/d/c/1.json",
        ],
    )


def _glob_with_not_exists_dir():
    """
    scenario: glob on a directory that is not exists
    expectation: if recursive is True,
        return the directory with postfix of slash('/'), otherwise, an empty list.
    keep identical result with standard glob module
    """

    assert_glob("/bucketForGlobTest/notExists/not_exists_file", [])
    assert_glob("/bucketForGlobTest/notExists/not_exists_dir/", [])

    # not exists path
    assert_glob("/notExistsBucket/**", [])

    assert_glob("/bucketA/notExists/**", [])

    assert_glob("/notExistsBucket/**", [])

    assert_glob("/bucketForGlobTest/notExists/**", [])


def _glob_with_dironly():
    """
    scenario: pathname with the postfix of slash('/')
    expectation: returns only contains pathname of directory,
        each of them is end with '/'
    """
    assert_glob(
        "/bucketForGlobTest/*/", ["/bucketForGlobTest/1/", "/bucketForGlobTest/2/"]
    )

    assert_glob("/bucketForGlobTest/[2-9]/", ["/bucketForGlobTest/2/"])

    # all sub-directories of 2, recursively
    assert_glob(
        "/bucketForGlobTest/2/**/*/",
        [
            "/bucketForGlobTest/2/a/",
            "/bucketForGlobTest/2/a/b/",
            "/bucketForGlobTest/2/a/b/a/",
            "/bucketForGlobTest/2/a/b/c/",
            "/bucketForGlobTest/2/a/d/",
            "/bucketForGlobTest/2/a/d/c/",
        ],
    )


def _glob_with_curly():
    """
    scenario: pathname with the curly braces('{}')
    expectation: returns only contains pathname of files
    """
    assert_glob(
        "/bucketForGlobTest/{1,2}/", ["/bucketForGlobTest/1/", "/bucketForGlobTest/2/"]
    )

    assert_glob("/bucketForGlobTest/{[2-4],[4-9]}/", ["/bucketForGlobTest/2/"])

    assert_glob(
        "/bucketForGlobTest/1/**/*.{json,msg}",
        [
            "/bucketForGlobTest/1/a/b/1.json",
            "/bucketForGlobTest/1/a/b/c/1.json",
            "/bucketForGlobTest/1/a/b/c/A.msg",
        ],
    )


def test_glob(fs_setup):
    _glob_with_common_wildcard()
    _glob_with_recursive_pathname()
    _glob_with_same_file_and_folder()
    _glob_with_nested_pathname()
    _glob_with_not_exists_dir()
    _glob_with_dironly()
    _glob_with_curly()


def test_escape():
    assert glob.escape("*") == "[*]"
    assert glob.escape("**") == "[*][*]"
    assert glob.escape("?") == "[?]"
    assert glob.escape("[]") == "[[]]"
    assert glob.escape("{}") == "[{]}"


def test_unescape():
    assert glob.unescape("[*]") == "*"
    assert glob.unescape("[*][*]") == "**"
    assert glob.unescape("[?]") == "?"
    assert glob.unescape("[[]]") == "[]"
    assert glob.unescape("[{]}") == "{}"


def test_globlize():
    path_list = [
        "/bucketForGlobTest/1",
        "/bucketForGlobTest/1/a",
        "/bucketForGlobTest/1/a/b",
        "/bucketForGlobTest/1/a/b/1.json",
        "/bucketForGlobTest/1/a/b/c",
        "/bucketForGlobTest/1/a/b/c/1.json",
        "/bucketForGlobTest/1/a/b/c/A.msg",
        "/bucketForGlobTest/2",
        "/bucketForGlobTest/2/a",
        "/bucketForGlobTest/2/a/b",
        "/bucketForGlobTest/2/a/b/a",
        "/bucketForGlobTest/2/a/b/a/1.json",
        "/bucketForGlobTest/2/a/b/c",
        "/bucketForGlobTest/2/a/b/c/1.json",
        "/bucketForGlobTest/2/a/b/c/2.json",
        "/bucketForGlobTest/2/a/d",
        "/bucketForGlobTest/2/a/d/2.json",
        "/bucketForGlobTest/2/a/d/c/1.json",
        "/bucketForGlobTest/2/a/d/c",
    ]
    new_path_list = list(fnmatch.filter(path_list, glob.globlize(path_list)))
    assert path_list == new_path_list

    path_list = [
        "/bucketForGlobTest/1.json",
        "/bucketForGlobTest/1/1.json",
        "/bucketForGlobTest/1/a/1.json",
    ]
    new_path_list = list(fnmatch.filter(path_list, glob.globlize(path_list)))
    assert "/bucketForGlobTest/{1,1/1,1/a/1}.json" == glob.globlize(path_list)
    assert path_list == new_path_list

    path_list = [
        "/bucketForGlobTest/b1.json",
        "/bucketForGlobTest/1/1.json",
        "/bucketForGlobTest/1/a/1.json",
    ]
    new_path_list = list(fnmatch.filter(path_list, glob.globlize(path_list)))
    assert "/bucketForGlobTest/{1/1,1/a/1,b1}.json" == glob.globlize(path_list)
    assert path_list == new_path_list

    path_list = [
        "/bucketForGlobTest1/b1.json",
        "/bucketForGlobTest/1/1.json",
        "/bucketForGlobTest/1/a/1.json",
    ]
    new_path_list = list(fnmatch.filter(path_list, glob.globlize(path_list)))
    assert (
        "/{bucketForGlobTest/1/1,bucketForGlobTest/1/a/1,bucketForGlobTest1/b1}.json"
        == glob.globlize(path_list)
    )
    assert path_list == new_path_list

    path_list = [
        "s3://bucketForGlobTest1/b1.json",
        "s3://bucketForGlobTest/1/1.json",
        "https://bucketForGlobTest/1/a/1.json",
    ]
    new_path_list = list(fnmatch.filter(path_list, glob.globlize(path_list)))
    assert (
        "{https://bucketForGlobTest/1/a/1,s3://bucketForGlobTest/1/1,"
        "s3://bucketForGlobTest1/b1}.json" == glob.globlize(path_list)
    )
    assert path_list == new_path_list

    path_list = [
        "/bucketForGlobTest/1.json",
        "/bucketForGlobTest/1/1.json",
        "/bucketForGlobTest/",
    ]
    new_path_list = list(fnmatch.filter(path_list, glob.globlize(path_list)))
    assert "/bucketForGlobTest/{,1.json,1/1.json}" == glob.globlize(path_list)
    assert sorted(path_list) == sorted(new_path_list)

    path_list = ["/bucketForGlobTest/1.json", "/bucketForGlobTest/1.json"]
    new_path_list = list(fnmatch.filter(path_list, glob.globlize(path_list)))
    assert "/bucketForGlobTest/1.json" == glob.globlize(path_list)
    assert path_list == new_path_list

    path_list = [
        "s3://bucketForGlobTest/a.nori/data",
        "s3://bucketForGlobTest/b.nori/data",
    ]
    new_path_list = list(fnmatch.filter(path_list, glob.globlize(path_list)))
    assert "s3://bucketForGlobTest/{a.nori,b.nori}/data" == glob.globlize(path_list)
    assert sorted(path_list) == new_path_list

    path_list = ["https://baidu.com/file", "https://google.com/file"]
    new_path_list = list(fnmatch.filter(path_list, glob.globlize(path_list)))
    assert "https://{baidu.com,google.com}/file" == glob.globlize(path_list)
    assert sorted(path_list) == new_path_list

    path_list = [
        "https://a.com/file.txt",
        "https://b.com/b.txt",
        "https://c.com/file.txt",
    ]
    new_path_list = list(fnmatch.filter(path_list, glob.globlize(path_list)))
    assert "https://{a.com/file,b.com/b,c.com/file}.txt" == glob.globlize(path_list)
    assert sorted(path_list) == new_path_list

    path_list = [
        "https://a[*].com/file.txt",
        "https://b.com/b.txt",
        "https://c.com/file.txt",
    ]
    assert "https://{a*.com/file,b.com/b,c.com/file}.txt" == glob.globlize(path_list)


def test_ungloblize():
    test_glob = (
        "{s3://facerec-raw-data-oss/v3/test-structure/20201207/1,"
        "s3://facerec-raw-data-oss/v3/test-structure/20201207/meta.msg,"
        "s3://facerec-raw-data-oss/v3/test-structure/20201207/meta.msg.idx}"
    )
    path_list = [
        "s3://facerec-raw-data-oss/v3/test-structure/20201207/1",
        "s3://facerec-raw-data-oss/v3/test-structure/20201207/meta.msg",
        "s3://facerec-raw-data-oss/v3/test-structure/20201207/meta.msg.idx",
    ]
    assert path_list == glob.ungloblize(test_glob)

    path_list = ["/bucketForGlobTest/1.json", "/bucketForGlobTest/1.json"]
    assert ["/bucketForGlobTest/1.json"] == glob.ungloblize(glob.globlize(path_list))

    path_list = [
        "s3://bucketForGlobTest/a.nori/data",
        "s3://bucketForGlobTest/b.nori/data",
    ]
    assert path_list == glob.ungloblize(glob.globlize(path_list))

    path_list = ["https://baidu.com/file", "https://google.com/file"]
    assert path_list == glob.ungloblize(glob.globlize(path_list))

    test_glob = "s3://bu-oss/a}b{a,b,c}.json"
    assert [
        "s3://bu-oss/a}ba.json",
        "s3://bu-oss/a}bb.json",
        "s3://bu-oss/a}bc.json",
    ] == glob.ungloblize(test_glob)

    test_glob = "s3://bu-oss/a,b{a,b,c}.json"
    assert [
        "s3://bu-oss/a,ba.json",
        "s3://bu-oss/a,bb.json",
        "s3://bu-oss/a,bc.json",
    ] == glob.ungloblize(test_glob)

    test_glob = "s3://bu-oss/a,b{a*,b,c}.json"
    assert [
        "s3://bu-oss/a,ba*.json",
        "s3://bu-oss/a,bb.json",
        "s3://bu-oss/a,bc.json",
    ] == glob.ungloblize(test_glob)

    test_glob = "s3://{empty,b}*Test{/,2/,3/sub/}{1/a/c,a/b}/2{.mp4,.jpg}/s"
    assert [
        "s3://empty*Test/1/a/c/2.mp4/s",
        "s3://empty*Test/1/a/c/2.jpg/s",
        "s3://empty*Test/a/b/2.mp4/s",
        "s3://empty*Test/a/b/2.jpg/s",
        "s3://empty*Test2/1/a/c/2.mp4/s",
        "s3://empty*Test2/1/a/c/2.jpg/s",
        "s3://empty*Test2/a/b/2.mp4/s",
        "s3://empty*Test2/a/b/2.jpg/s",
        "s3://empty*Test3/sub/1/a/c/2.mp4/s",
        "s3://empty*Test3/sub/1/a/c/2.jpg/s",
        "s3://empty*Test3/sub/a/b/2.mp4/s",
        "s3://empty*Test3/sub/a/b/2.jpg/s",
        "s3://b*Test/1/a/c/2.mp4/s",
        "s3://b*Test/1/a/c/2.jpg/s",
        "s3://b*Test/a/b/2.mp4/s",
        "s3://b*Test/a/b/2.jpg/s",
        "s3://b*Test2/1/a/c/2.mp4/s",
        "s3://b*Test2/1/a/c/2.jpg/s",
        "s3://b*Test2/a/b/2.mp4/s",
        "s3://b*Test2/a/b/2.jpg/s",
        "s3://b*Test3/sub/1/a/c/2.mp4/s",
        "s3://b*Test3/sub/1/a/c/2.jpg/s",
        "s3://b*Test3/sub/a/b/2.mp4/s",
        "s3://b*Test3/sub/a/b/2.jpg/s",
    ] == glob.ungloblize(test_glob)

    path_list = [
        "/bucketForGlobTest/1.json",
        "/bucketForGlobTest/1/1.json",
        "/bucketForGlobTest/",
    ]
    assert sorted(path_list) == sorted(glob.ungloblize(glob.globlize(path_list)))

    assert glob.ungloblize("s3://{a*{},b*}/1/2/*") == ["s3://a*[{],b*}/1/2/*"]


def test_get_no_glob_root_path():
    assert glob.get_non_glob_dir("/data/**/*.py") == "/data"
    assert glob.get_non_glob_dir("/**/*.py") == "/"
    assert glob.get_non_glob_dir("./**/*.py") == "."
    assert glob.get_non_glob_dir("**/*.py") == "."


def test__iglob():
    with pytest.raises(OSError):
        list(glob._iglob("/root", True, dironly=True, fs=glob.DEFAULT_FILESYSTEM_FUNC))


def test__glob2():
    with pytest.raises(OSError):
        list(glob._glob2("/root", "", dironly=True, fs=glob.DEFAULT_FILESYSTEM_FUNC))


# def test_has_magic_ignore_brace():
