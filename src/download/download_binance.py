
from urllib.request import urlretrieve
from pathlib import Path


def download_bronze(url: str, destination: Path):

    destination.parent.mkdir(parents = True ,exist_ok = True)
    if destination.exists():
        print(f"The file already exist at {destination.parent}")
        return
    print(f"downloading the File at {destination.parent}")
    urlretrieve(url, destination)

    print(f"The download is complete at {destination.parent}")


