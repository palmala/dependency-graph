import logging
from abc import ABC, abstractmethod
import pandas as pd
from collections import defaultdict


class ProjectsProvider(ABC):

    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def get_projects(self) -> dict:
        pass


class MavenProjectsProvider(ProjectsProvider):

    def __init__(self, csv_file: str):
        try:
            self.df = pd.read_csv(csv_file)
            logging.info(f'Data read: \n' + self.df.head().to_string(index=False))
        except Exception as err:
            logging.error(f"Could not create df from {csv_file} due to: {err}")
            self.df = pd.DataFrame()

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
        # logging.info("Projects of interest: " + "\n".join(projects_of_interest))

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

    @classmethod
    def __concat_columns(cls, row: dict, column_a: str, column_b: str):
        return row[column_a] + "/" + row[column_b]
