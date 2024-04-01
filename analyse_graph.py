from maven_xmls import collect_maven_xmls, process_maven_xmls
from dotbuilder import *
from projects_provider import MavenProjectsProvider
import logging_config

import os
import shutil

# SOURCE = 'https://packages.atlassian.com/mvn/maven-atlassian-external/com/hazelcast/'
# SOURCE = 'https://repo1.maven.org/maven2/com/google/'
SOURCES = [
    'https://repo1.maven.org/maven2/hu/bme/mit/theta/',
    'https://maven.pkg.jetbrains.space/public/p/ktor/eap/io/ktor/'
]
OUTPUT = "build"
FILENAMES = "{basedir}/{prefix}_{filename}.dot"
logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    shutil.rmtree(OUTPUT, ignore_errors=True)
    os.makedirs(OUTPUT)

    for source in SOURCES:
        elements = source.split("/")
        results_file_prefix = elements[-3] + "." + elements[-2]
        results_file = f"{OUTPUT}/{results_file_prefix}.release_details.csv"

        maven_xmls = collect_maven_xmls(source)
        release_detail_records = process_maven_xmls(maven_xmls, results_file=results_file)
        projects = MavenProjectsProvider(release_detail_records).get_projects()

        logging.info(f"Analysing graph for {source}")
        subject = dot_builder(projects, "base_projects")
        write_to_file(subject, FILENAMES.format(basedir=OUTPUT, prefix=results_file_prefix, filename="base_projects"))

        instability = calculate_instability(subject)
        calculate_violations(subject, instability)
        write_to_file(subject,
                      FILENAMES.format(basedir=OUTPUT, prefix=results_file_prefix, filename="base_projects_violations"))

        cycles = detect_all_cycles(subject)
