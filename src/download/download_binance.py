
from urllib.request import urlretrieve
from pathlib import Path

import zipfile

def download_bronze(url: str, destination: Path):

    destination.parent.mkdir(parents = True ,exist_ok = True)
    if destination.exists():
        print(f"The file already exist at {destination.parent}")
        return
    print(f"downloading the File at {destination.parent}")
    urlretrieve(url, destination)

    print(f"The download is complete at {destination.parent}")


def unzip(destination: Path, directory_to_extract: Path):

    extracted_file = directory_to_extract / f"{destination.stem}.csv"
    if extracted_file.exists():
        print(f"you don't need to unzip to {directory_to_extract} because it already exist")
        return

    directory_to_extract.parent.mkdir(parents = True, exist_ok = True)

    with zipfile.ZipFile(destination, 'r') as zip:
        zip.extractall(directory_to_extract)

    print(f"File extracted to: {directory_to_extract}")
