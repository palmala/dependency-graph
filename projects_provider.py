import logging
from abc import ABC, abstractmethod
import pandas as pd
from collections import defaultdict
from functools import lru_cache
import requests
import xml.etree.ElementTree as ET
import re
import multiprocessing

STORE_RESULTS = 'release_details.csv'


class ProjectsProvider(ABC):

    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def get_projects(self) -> dict:
        pass


class MavenProjectsProvider(ProjectsProvider):

    def __init__(self, maven_xmls: list):
        release_details = self.process_maven_xmls(maven_xmls)
        self.df = pd.DataFrame.from_records(release_details)
        self.__build_graph()

    def get_projects(self) -> dict:
        return self.graph

    def __build_graph(self):
        if self.df.empty:
            return dict()

        self.graph = defaultdict(set)
        self.df['project'] = self.df.apply(self.__get_project_name, axis=1)
        self.df['dep_project'] = self.df.apply(self.__get_dep_project_name, axis=1)
        projects_of_interest = set(self.df['project'].to_list())

        for index, row in self.df.iterrows():
            proj = row['project']
            dep = row['dep_project']
            if dep in projects_of_interest and proj != dep:
                self.graph[proj].add(dep)

    @classmethod
    def __get_project_name(cls, row: dict):
        return cls.__concat_columns(row, 'group_id', 'artifact_id')

    @classmethod
    def __get_dep_project_name(cls, row: dict):
        return cls.__concat_columns(row, 'dep_group_id', 'dep_artifact_id')

    @staticmethod
    def __concat_columns(row: dict, column_a: str, column_b: str):
        return row[column_a] + "/" + row[column_b]

    @classmethod
    def process_maven_xml(cls, xml_url: str) -> dict:
        logging.debug(f"Processing {xml_url}")
        with cls.get_http_session() as session:
            response = session.get(xml_url)
            page = response.text

            root = ET.fromstring(page)
            try:
                group_id = root.findall("./groupId")[0].text
                artifact_id = root.findall("./artifactId")[0].text
                latest = cls.extract_latest_version(root, xml_url)
            except Exception as exp:
                logging.error(f'Failed to extract data from {xml_url} due to : {exp}')
                return dict()

            basedir = xml_url.removesuffix('/maven-metadata.xml')
            release_details_xml = f"{basedir}/{latest}/{artifact_id}-{latest}.pom"
            deps = cls.get_dependencies(release_details_xml)
            if not deps:
                logging.warning(f"No dependencies for {release_details_xml}")

        return {'group_id': group_id, 'artifact_id': artifact_id, 'latest': latest,
                'release_details_xml': release_details_xml, 'dependencies': deps}

    @staticmethod
    @lru_cache(maxsize=None)
    def get_http_session():
        return requests.Session()

    @staticmethod
    def extract_latest_version(maven_xml: ET, url: str) -> str:
        try:
            latest = maven_xml.findall("./versioning/latest")[0].text
        except Exception as err:
            logging.error(f"Failed to extract latest version from {url} with error: {err}")
            latest = "ERROR"

        return latest

    @classmethod
    def get_dependencies(cls, pom_file):
        deps = []
        session = cls.get_http_session()
        try:
            response = session.get(pom_file).text
            root = ET.fromstring(response)
            ns = cls.namespace(root)
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

    @staticmethod
    def namespace(element):
        m = re.match(r'\{.*\}', element.tag)
        return m.group(0) if m else ''

    @classmethod
    def process_maven_xmls(cls, maven_xmls_records: list, results_file: str = STORE_RESULTS) -> list:
        pool = multiprocessing.Pool(8)

        maven_xmls = [row['maven_xml'] for row in maven_xmls_records]
        logging.info(f"Gathering results for {len(maven_xmls)} maven xmls")

        tasks = list()
        results = list()

        for maven_xml in maven_xmls:
            t = pool.apply_async(cls.process_maven_xml, (maven_xml,), callback=results.append)
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
