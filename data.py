import time
from bs4 import BeautifulSoup
import requests
import logging
import random
from functools import lru_cache

SOURCE = 'https://repo1.maven.org/maven2/'
logging.basicConfig(encoding='utf-8', level=logging.INFO)


@lru_cache(maxsize=None)
def listFD(url: str) -> list:
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')
    return [url + node.get('href') for node in soup.find_all('a') if
            node.get('href')]


def get_directories(url: str):
    return [element for element in listFD(url) if element.endswith("/") and not element.endswith("/../")]


def get_xmls(url: str):
    return [element for element in listFD(url) if element.endswith('xml')]


def find_maven_metadata(url: str):
    return [element for element in get_xmls(url) if element.endswith('maven-metadata.xml')]


def main():
    start = time.perf_counter()
    logging.info(f"Starting query {SOURCE}")

    directories = get_directories(SOURCE)
    logging.info(f"Got {len(directories)} results")

    to_process = list(random.choices(directories, k=100))
    xmls = list()
    while to_process:
        current_dir = to_process.pop()
        # logging.info(f"Querying {current_dir}")
        logging.info(f"to_process size: {len(to_process)}, xmls size: {len(xmls)}")

        metadata = get_xmls(current_dir)
        if metadata:
            # logging.info(f"Metadata found {metadata}")
            xmls.extend(metadata)
            continue

        next_dirs = get_directories(current_dir)
        if next_dirs:
            to_process.extend(next_dirs)

    logging.info(f"Got {len(xmls)} results")
    logging.info("\n" + "\n".join(xmls))

    run_length = time.perf_counter() - start
    logging.info(f"Execution took {run_length} seconds")


if __name__ == "__main__":
    main()
