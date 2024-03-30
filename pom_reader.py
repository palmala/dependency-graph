import xml.etree.ElementTree as ET


def main():
    tree = ET.parse('example.pom')
    root = tree.getroot()
    dependencies = root.findall('./dependency')
    if dependencies:
        for dependency in root.findall('./dependency'):
            group_id = dependency.find('groupId').text
            artifact_id = dependency.find('artifactId').text
            version = dependency.find('version').text
            print(group_id, artifact_id,version)
    else:
        print("Nope!")

if __name__ == "__main__":
    main()
