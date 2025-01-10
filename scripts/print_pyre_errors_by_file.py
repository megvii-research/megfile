import json
import os

if __name__ == "__main__":
    file_path = os.path.join(os.path.dirname(__file__), "..", "pyre-sarif.json")
    with open(file_path) as f:
        data = json.load(f)

    index = 1
    for run in data["runs"]:
        for result in run["results"]:
            print(f"[{index}] {result['message']['text']}")
            for location in result["locations"]:
                print(
                    ":".join(
                        [
                            location["physicalLocation"]["artifactLocation"]["uri"],
                            str(location["physicalLocation"]["region"]["startLine"]),
                            str(location["physicalLocation"]["region"]["startColumn"]),
                        ]
                    )
                )
            index += 1
            print("\n")
