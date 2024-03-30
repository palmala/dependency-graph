import time
from bs4 import BeautifulSoup
import logging
import random
from functools import lru_cache
import multiprocessing
import requests
import pandas as pd

import logging_config

# SOURCE = 'https://repo1.maven.org/maven2/'
SOURCE = 'https://maven.pkg.jetbrains.space/public/p/ktor/eap/io/ktor/'


@lru_cache(maxsize=None)
def get_session():
    return requests.Session()


def listFD(url: str) -> list:
    with requests.Session() as session:
        with session.get(url) as response:
            page = response.text
            soup = BeautifulSoup(page, 'html.parser')
            result = [node.get('href') for node in soup.find_all('a') if
                      node.get('href') and node.get('href') not in url]
            return result


def process_dir(directory):
    elements = [element for element in listFD(directory) if not element.endswith("/../")]
    maven_xmls = [element for element in elements if element.endswith('maven-metadata.xml')]
    directories = [element for element in elements if element.endswith("/")]
    # logging.info(f"xmls: {len(maven_xmls)} dirs: {len(directories)}")
    if maven_xmls:
        return maven_xmls

    for directory in directories:
        asd = process_dir(directory)
        # logging.info(f"Got {len(asd)} for {directory}")
        maven_xmls.extend(asd)

    return maven_xmls


def new_main():
    main_directories = sorted([element for element in listFD(SOURCE) if
                               element.endswith("/") and not element.endswith("/../")])
    logging.info(f"Read {len(main_directories)} main directories")

    try:
        progress = pd.read_csv('xmls.csv')
        processed = set(progress['main_dir'].to_list())
    except FileNotFoundError:
        progress = pd.DataFrame()
        processed = set()

    total = len(main_directories)
    to_process = [directory for directory in main_directories if directory not in processed]
    logging.info(f"To process {len(to_process)}")
    processed = 0
    while to_process:
        current_dir = to_process.pop()
        logging.info(f"Processing {current_dir}")
        results = process_dir(current_dir)
        xmls = [{'main_dir': current_dir, 'maven_xml': maven_xml} for maven_xml in results]
        logging.info(f"Results for {current_dir}: {len(results)}, remaining: {len(to_process)}")

        if not results:
            xmls = [{'main_dir': current_dir, 'maven_xml': ""}]

        if not progress.empty:
            progress = pd.concat([progress, pd.DataFrame.from_records(xmls)])
        else:
            progress = pd.DataFrame.from_records(xmls)

        progress.to_csv('xmls.csv', index=False)

        processed += 1
        # if processed == 100:
        #     break


def testme():
    logging.info(process_dir('https://maven.pkg.jetbrains.space/public/p/ktor/eap/io/ktor/plugin/'))


def main():
    start = time.perf_counter()
    logging.info(f"Starting query {SOURCE}")
    new_main()
    run_length = time.perf_counter() - start
    logging.info(f"Execution took {run_length} seconds")


if __name__ == "__main__":
    main()
