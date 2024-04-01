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


def extract_latest_version(maven_xml: ET, url: str) -> str:
    try:
        latest = maven_xml.findall("./versioning/latest")[0].text
    except Exception as err:
        logging.error(f"Failed to extract latest version from {url} with error: {err}")
        latest = "ERROR"

    return latest


def namespace(element):
    m = re.match(r'\{.*\}', element.tag)
    return m.group(0) if m else ''


def get_dependencies(pom_file):
    deps = []
    session = get_http_session()
    try:
        response = session.get(pom_file).text
        root = ET.fromstring(response)
        ns = namespace(root)
        dependencies = root.find(ns + 'dependencies')
        if dependencies is None:
            dependencies = root.find(ns + 'dependencyManagement/' + ns + 'dependencies')
        for dependency in dependencies:
            dep_group_id = dependency.find(ns + 'groupId').text
            dep_artifact_id = dependency.find(ns + 'artifactId').text
            dep_version = dependency.find(ns + 'version').text
            deps.append({'group_id': dep_group_id, 'artifact_id': dep_artifact_id, 'version': dep_version})
    except Exception as err:
        logging.error(f"Could not get dependencies for {pom_file} due to error: {err}")

    return deps


def process_maven_xml(xml_url: str) -> dict:
    logging.debug(f"Processing {xml_url}")
    with get_http_session() as session:
        response = session.get(xml_url)
        page = response.text

        root = ET.fromstring(page)
        try:
            group_id = root.findall("./groupId")[0].text
            artifact_id = root.findall("./artifactId")[0].text
            latest = extract_latest_version(root, xml_url)
        except Exception as exp:
            logging.error(f'Failed to extract data from {xml_url} due to : {exp}')
            return dict()

        basedir = xml_url.removesuffix('/maven-metadata.xml')
        release_details_xml = f"{basedir}/{latest}/{artifact_id}-{latest}.pom"
        deps = get_dependencies(release_details_xml)
        if not deps:
            logging.warning(f"No dependencies for {release_details_xml}")

    return {'group_id': group_id, 'artifact_id': artifact_id, 'latest': latest,
            'release_details_xml': release_details_xml, 'dependencies': deps}


def process_maven_xmls(maven_xmls_records: list, results_file: str = STORE_RESULTS) -> list:
    pool = multiprocessing.Pool(8)

    maven_xmls = [row['maven_xml'] for row in maven_xmls_records]
    logging.info(f"Gathering results for {len(maven_xmls)} maven xmls")

    tasks = list()
    results = list()

    for maven_xml in maven_xmls:
        t = pool.apply_async(process_maven_xml, (maven_xml,), callback=results.append)
        tasks.append(t)

    for t in tasks:
        try:
            t.wait()
        except Exception as err:
            print(f"[ERROR]: Exception during task exec: {err}")

    results = [res for res in results if res]
    records = list()
    for res in results:
        for dep in res['dependencies']:
            records.append({
                'group_id': res['group_id'],
                'artifact_id': res['artifact_id'],
                'latest': res['latest'],
                'release_details_xml': res['release_details_xml'],
                'dep_group_id': dep['group_id'],
                'dep_artifact_id': dep['artifact_id'],
                'dep_version': dep['version']
            })

    logging.info(f"Got results for {len(results)} artifacts")
    logging.info(f"Writing CSV: {results_file}")
    df = pd.DataFrame.from_records(records)
    df.to_csv(results_file, index=False)
    return records
