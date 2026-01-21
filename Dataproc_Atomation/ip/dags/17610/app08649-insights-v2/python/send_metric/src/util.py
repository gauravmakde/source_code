import yaml


def read_yaml(yaml_file_path):
    try:
        with open(yaml_file_path, 'r') as file:
            yaml_input = yaml.safe_load(file)
        return yaml_input
    except FileNotFoundError as e:
        print("ERROR: File with this name does not exist!")
        raise e
    except yaml.parser.ParserError as e:
        print("ERROR: Check the YAML formatting!")
        raise e
    except yaml.scanner.ScannerError as e:
        print("ERROR: Check the YAML formatting!")
        raise e
