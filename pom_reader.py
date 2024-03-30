import xml.etree.ElementTree as ET
import re


def namespace(element):
    m = re.match(r'\{.*\}', element.tag)
    return m.group(0) if m else ''


def main():
    root = ET.parse('example.pom')
    ns = namespace(root.getroot())
    dependencies = root.find(ns + 'dependencies')
    for dependency in dependencies:
        group_id = dependency.find(ns + 'groupId').text
        artifact_id = dependency.find(ns + 'artifactId').text
        version = dependency.find(ns + 'version').text
        print(group_id, artifact_id, version)


if __name__ == "__main__":
    main()
