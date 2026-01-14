import os
import shutil

SRC_DOCS_PATH = "docs"
SRC_FILE_NAMES = [
    "path_format.md",
    "configuration/",
    "advanced/glob.md",
]
MARKDOWN_DOCS_PATH = "markdown_doc/markdown"
SPHINX_FILE_NAMES = [
    "megfile.smart.md",
    "megfile.smart_path.md",
    "cli.md",
]
SKILL_OUTPUT_PATH = "markdown_doc/skill"

if __name__ == "__main__":
    references_path = os.path.join(SKILL_OUTPUT_PATH, "references")
    os.makedirs(references_path, exist_ok=True)

    # copy SKILL.md
    skill_md_path = os.path.join(os.path.dirname(__file__), "SKILL.md")
    shutil.copy(skill_md_path, os.path.join(SKILL_OUTPUT_PATH, "SKILL.md"))

    # copy reference markdown files
    for filename in SPHINX_FILE_NAMES:
        src_path = os.path.join(MARKDOWN_DOCS_PATH, filename)
        dst_path = os.path.join(references_path, filename)
        os.makedirs(os.path.dirname(dst_path), exist_ok=True)
        if filename.endswith("/"):
            shutil.copytree(src_path, dst_path, dirs_exist_ok=True)
        else:
            shutil.copy(src_path, dst_path)

    # copy src docs markdown files
    for filename in SRC_FILE_NAMES:
        src_path = os.path.join(SRC_DOCS_PATH, filename)
        dst_path = os.path.join(references_path, filename)
        os.makedirs(os.path.dirname(dst_path), exist_ok=True)
        if filename.endswith("/"):
            shutil.copytree(src_path, dst_path, dirs_exist_ok=True)
        else:
            shutil.copy(src_path, dst_path)

    print("Skill documentation generated successfully.")
