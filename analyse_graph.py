from dotbuilder import *
from projects_provider import MavenProjectsProvider

import os
import shutil
import logging

OUTPUT = "build"
FILENAMES = "{basedir}/{filename}.dot"
logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    shutil.rmtree(OUTPUT, ignore_errors=True)
    os.makedirs(OUTPUT)

    logging.info("Getting projects")
    projects = MavenProjectsProvider('release_details.csv').get_projects()

    logging.info("Creating base graph")
    subject = dot_builder(projects, "base_projects")
    write_to_file(subject, FILENAMES.format(basedir=OUTPUT, filename="base_projects"))

    instability = calculate_instability(subject)
    calculate_violations(subject, instability)
    write_to_file(subject, FILENAMES.format(basedir=OUTPUT, filename="base_projects_violations"))

    cycles = detect_all_cycles(subject)
