import sys

from megfile.lib import fnmatch


def test_translate():
    # wildcard
    assert fnmatch.translate("?") == fnmatch._compat(r".")
    assert fnmatch.translate("*") == fnmatch._compat(r"[^/]*")
    assert fnmatch.translate("**") == fnmatch._compat(r".*")
    assert fnmatch.translate("**/a") == fnmatch._compat(r"(.*/)?a")
    if sys.version_info > (3, 7):
        assert fnmatch.translate("b**/a") == fnmatch._compat(r"b.*/a")
        assert fnmatch.translate("b/**/a") == fnmatch._compat(r"b/(.*/)?a")
        assert fnmatch.translate("c/b**/a") == fnmatch._compat(r"c/b.*/a")
    else:
        assert fnmatch.translate("b**/a") == fnmatch._compat(r"b.*\/a")
        assert fnmatch.translate("b/**/a") == fnmatch._compat(r"b\/(.*/)?a")
        assert fnmatch.translate("c/b**/a") == fnmatch._compat(r"c\/b.*\/a")

    # brackets
    assert fnmatch.translate("[abc]") == fnmatch._compat(r"[abc]")
    assert fnmatch.translate("[!abc]") == fnmatch._compat(r"[^abc]")
    assert fnmatch.translate("[^abc]") == fnmatch._compat(r"[\^abc]")
    assert fnmatch.translate("[abc^]") == fnmatch._compat(r"[abc^]")
    assert fnmatch.translate("[abc?]") == fnmatch._compat(r"[abc?]")
    assert fnmatch.translate("[a-z]") == fnmatch._compat(r"[a-z]")
    assert fnmatch.translate("[]") == fnmatch._compat(r"\[\]")
    if sys.version_info > (3, 7):
        assert fnmatch.translate("[!]") == fnmatch._compat(r"\[!\]")
    else:
        assert fnmatch.translate("[!]") == fnmatch._compat(r"\[\!\]")
    assert fnmatch.translate("[") == fnmatch._compat(r"\[")

    # curly braces
    assert fnmatch.translate("{a,b}") == fnmatch._compat(r"(a|b)")
    assert fnmatch.translate("{a, b}") == fnmatch._compat(r"(a|\ b)")
    assert fnmatch.translate("{}") == fnmatch._compat(r"\{\}")
    assert fnmatch.translate("{,}") == fnmatch._compat(r"(|)")
    assert fnmatch.translate("{") == fnmatch._compat(r"\{")

    # weirdos
    assert fnmatch.translate("(a|b)") == fnmatch._compat(r"\(a\|b\)")
    assert fnmatch.translate("{*,d}") == fnmatch._compat(r"([^/]*|d)")
    assert fnmatch.translate("{**,d}") == fnmatch._compat(r"(.*|d)")
    assert fnmatch.translate("{[abc],d}") == fnmatch._compat(r"([abc]|d)")
    if sys.version_info > (3, 7):
        assert fnmatch.translate("{{a,b},d}") == fnmatch._compat(r"(\{a|b),d\}")
    else:
        assert fnmatch.translate("{{a,b},d}") == fnmatch._compat(r"(\{a|b)\,d\}")


def test_filter():
    file_list = [
        "a",
        "b",
        "c/d",
        "d",
        "*",
        "**",
        "[abc]",
        "(a|b)",
        "{a",
        "b}",
        "{a,d}",
        "[",
        "]",
        "[]",
        "[!]",
        "{",
        "}",
        "{}",
        "{,}",
        "^",
        "!",
        "?",
        ",",
    ]

    # wildcard
    assert fnmatch.filter(file_list, "?") == [
        "a",
        "b",
        "d",
        "*",
        "[",
        "]",
        "{",
        "}",
        "^",
        "!",
        "?",
        ",",
    ]
    assert fnmatch.filter(file_list, "*") == [
        "a",
        "b",
        "d",
        "*",
        "**",
        "[abc]",
        "(a|b)",
        "{a",
        "b}",
        "{a,d}",
        "[",
        "]",
        "[]",
        "[!]",
        "{",
        "}",
        "{}",
        "{,}",
        "^",
        "!",
        "?",
        ",",
    ]
    assert fnmatch.filter(file_list, "**") == [
        "a",
        "b",
        "c/d",
        "d",
        "*",
        "**",
        "[abc]",
        "(a|b)",
        "{a",
        "b}",
        "{a,d}",
        "[",
        "]",
        "[]",
        "[!]",
        "{",
        "}",
        "{}",
        "{,}",
        "^",
        "!",
        "?",
        ",",
    ]
    assert fnmatch.filter(file_list, "**/d") == ["c/d", "d"]

    # brackets
    assert fnmatch.filter(file_list, "[abc]") == ["a", "b"]
    assert fnmatch.filter(file_list, "[!abc]") == [
        "d",
        "*",
        "[",
        "]",
        "{",
        "}",
        "^",
        "!",
        "?",
        ",",
    ]
    assert fnmatch.filter(file_list, "[^abc]") == ["a", "b", "^"]
    assert fnmatch.filter(file_list, "[abc^]") == ["a", "b", "^"]
    assert fnmatch.filter(file_list, "[abc?]") == ["a", "b", "?"]
    assert fnmatch.filter(file_list, "[a-z]") == ["a", "b", "d"]
    assert fnmatch.filter(file_list, "[]") == ["[]"]
    assert fnmatch.filter(file_list, "[!]") == ["[!]"]
    assert fnmatch.filter(file_list, "[") == ["["]

    # curly braces
    assert fnmatch.filter(file_list, "{a,b}") == ["a", "b"]
    assert fnmatch.filter(file_list, "{a, b}") == ["a"]
    assert fnmatch.filter(file_list, "{}") == ["{}"]
    assert fnmatch.filter(file_list, "{,}") == []
    assert fnmatch.filter(file_list, "{") == ["{"]

    # weirdos
    assert fnmatch.filter(file_list, "(a|b)") == ["(a|b)"]
    assert fnmatch.filter(file_list, "{*,d}") == [
        "a",
        "b",
        "d",
        "*",
        "**",
        "[abc]",
        "(a|b)",
        "{a",
        "b}",
        "{a,d}",
        "[",
        "]",
        "[]",
        "[!]",
        "{",
        "}",
        "{}",
        "{,}",
        "^",
        "!",
        "?",
        ",",
    ]
    assert fnmatch.filter(file_list, "{**,d}") == [
        "a",
        "b",
        "c/d",
        "d",
        "*",
        "**",
        "[abc]",
        "(a|b)",
        "{a",
        "b}",
        "{a,d}",
        "[",
        "]",
        "[]",
        "[!]",
        "{",
        "}",
        "{}",
        "{,}",
        "^",
        "!",
        "?",
        ",",
    ]
    assert fnmatch.filter(file_list, "{[abc],d}") == [
        "a",
        "b",
        "d",
    ]
    assert fnmatch.filter(file_list, "{{a,b},d}") == ["{a,d}"]


def test_fnmatch():
    assert fnmatch.fnmatch("a", "{a,b}")
    assert fnmatch.fnmatch("b", "{a,b}")
    assert not fnmatch.fnmatch("A", "{a,b}")
    assert not fnmatch.fnmatchcase("A", "{a,b}")
