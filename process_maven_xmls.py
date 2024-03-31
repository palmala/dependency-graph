import logging
import multiprocessing
import time
import requests
import pandas as pd
import requests
from functools import lru_cache
import logging_config
import xml.etree.ElementTree as ET
import re


@lru_cache(maxsize=None)
def get_http_session():
    return requests.Session()


def extract_latest_version(maven_xml: ET, url: str) -> str:
    try:
        latest = maven_xml.findall("./versioning/latest")[0].text
    except Exception as err:
        logging.error(f"Failed to extract latest version from {url}")
        latest = "ERROR"

    return latest


def namespace(element):
    m = re.match(r'\{.*\}', element.tag)
    return m.group(0) if m else ''


def process_maven_xml(xml_url: str) -> dict:
    logging.info(f'Processing {xml_url}')
    with get_http_session() as session:
        response = session.get(xml_url)
        page = response.text

        root = ET.fromstring(page)
        try:
            group_id = root.findall("./groupId")[0].text
            artifact_id = root.findall("./artifactId")[0].text
            latest = extract_latest_version(root, xml_url)
        except Exception as exp:
            logging.error(f'Failed to extract data from {xml_url}')
            return dict()

        basedir = xml_url.removesuffix('/maven-metadata.xml')
        release_details_xml = f"{basedir}/{latest}/{artifact_id}-{latest}.pom"
        deps = []
        try:
            response = session.get(release_details_xml).text
            root = ET.fromstring(response)
            ns = namespace(root)
            dependencies = root.find(ns + 'dependencies')
            for dependency in dependencies:
                group_id = dependency.find(ns + 'groupId').text
                artifact_id = dependency.find(ns + 'artifactId').text
                version = dependency.find(ns + 'version').text
                deps.append({'group_id': group_id, 'artifact_id': artifact_id, 'version': version})
        except Exception as err:
            logging.error(f"Could not get dependencies for {release_details_xml}")

    return {'group_id': group_id, 'artifact_id': artifact_id, 'latest': latest,
            'release_details_xml': release_details_xml, 'dependencies': deps}


def get_data_maven_xmls() -> pd.DataFrame:
    try:
        df = pd.read_csv('jetbrains.xmls.csv')
    except Exception as exp:
        logging.error('Nope!')
        exit(1)

    if df.empty:
        logging.info("No data in csv file.")
        exit(0)

    return df


def main():
    start = time.perf_counter()

    df = get_data_maven_xmls()
    pool = multiprocessing.Pool(8)

    logging.info(f"Gathering results for {len(df)} artifacts")
    try:
        latest_release_data = pd.read_csv('latest_release_data.csv')
        processed_xmls = set(latest_release_data['maven_xml'].to_list())
    except Exception as err:
        latest_release_data = pd.DataFrame()
        processed_xmls = set()

    logging.info(f"Got results from previous run for {len(processed_xmls)} artifacts")

    tasks = list()
    results = list()
    count = 0
    for index, row in df.iterrows():

        current_xml = row['maven_xml']
        if current_xml in processed_xmls:
            logging.info(f"Using previous results for {current_xml}")
            continue
        t = pool.apply_async(process_maven_xml, (current_xml,), callback=results.append)
        tasks.append(t)

        # count += 1
        # if count == 5:
        #     break

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
    logging.info(f"Writing CSV: release_details.csv")
    df = pd.DataFrame.from_records(records)
    df.to_csv('release_details.csv', index=False)

    run_length = time.perf_counter() - start
    logging.info(f"Execution took {run_length} seconds")


def test_me():
    logging.info(process_maven_xml(
        'https://maven.pkg.jetbrains.space/public/p/ktor/eap/io/ktor/plugin/plugin/maven-metadata.xml'))


if __name__ == "__main__":
    main()
