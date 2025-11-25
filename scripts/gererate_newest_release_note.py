action_map = {
    "feat": "New features",
    "fix": "Bug fixes",
    "perf": "Performance improvements",
    "docs": "Documentation",
    "refactor": "Refactorings",
    "style": "Style",
    "test": "Tests",
    "chore": "Maintenance",
    "ci": "CI",
    "revert": "Reverted",
    "breaking change": "Breaking Changes",
}


with open("CHANGELOG.md", "r") as f:
    for i, line in enumerate(f):
        line = line.strip("\n")
        if line == "":
            break
        if line.startswith("## "):
            continue
        if line.startswith("- "):
            action_key = line[2:].strip().strip("*")
            action_desc = action_map.get(action_key, action_key)
            if i > 2:
                print()
            print(f"## {line[2:].replace(action_key, action_desc)}")
        elif line.startswith("    "):
            print(line[4:])
        else:
            raise ValueError(f"Unrecognized line format: {line}")
