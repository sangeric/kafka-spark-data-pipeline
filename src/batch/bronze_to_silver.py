from pathlib import Path
import zipfile


def unzip(destination: Path, directory_to_extract: Path):

    extracted_file = directory_to_extract / f"{destination.stem}.csv"
    if extracted_file.exists():
        print(f"you don't need to unzip to {directory_to_extract} because it already exist")
        return

    directory_to_extract.parent.mkdir(parents = True, exist_ok = True)

    with zipfile.ZipFile(destination, 'r') as zip:
        zip.extractall(directory_to_extract)

    print(f"File extracted to: {directory_to_extract}")
