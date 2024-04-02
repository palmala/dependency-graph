import requests
from functools import lru_cache
from bs4 import BeautifulSoup
import logging
import pandas as pd
import re
import xml.etree.ElementTree as ET
import multiprocessing

STORE_RESULTS = 'release_details.csv'


@lru_cache(maxsize=None)
def get_http_session():
    return requests.Session()


def listFD(url: str) -> list:
    with get_http_session() as session:
        with session.get(url) as response:
            page = response.text
            soup = BeautifulSoup(page, 'html.parser')
            results = list()
            for node in soup.find_all('a'):
                href = node.get('href')
                if href not in url:
                    if href.startswith("http://") or href.startswith("https://"):
                        results.append(href)
                    else:
                        results.append(url + href)
            return results


def process_dir(directory):
    elements = [element for element in listFD(directory) if not element.endswith("/../")]
    maven_xmls = [element for element in elements if element.endswith('maven-metadata.xml')]
    directories = [element for element in elements if element.endswith("/")]
    if maven_xmls:
        return maven_xmls

    for directory in directories:
        maven_xmls.extend(process_dir(directory))

    return maven_xmls


def collect_maven_xmls(source: str) -> list:
    logging.info(f"Started collecting maven xmls from {source}")
    main_directories = sorted([element for element in listFD(source) if
                               element.endswith("/") and not element.endswith("/../")])
    logging.info(f"Read {len(main_directories)} main directories")

    xmls = list()
    to_check = 0
    while main_directories:
        current_dir = main_directories.pop()
        logging.debug(f"Processing {current_dir}")
        results = process_dir(current_dir)
        current = [{'main_dir': current_dir, 'maven_xml': maven_xml} for maven_xml in results]
        logging.debug(f"Results for {current_dir}: {len(results)}, remaining: {len(main_directories)}")

        if not results:
            current = [{'main_dir': current_dir, 'maven_xml': ""}]
        xmls.extend(current)
        # to_check += 1
        # if to_check == 20:
        #     break

    return xmls















