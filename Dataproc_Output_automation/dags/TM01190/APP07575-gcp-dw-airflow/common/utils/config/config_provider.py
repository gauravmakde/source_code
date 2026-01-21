import os
import sys

current_path = os.path.split(__file__)[0]
sys.path.append(current_path)

from section_aware_config_parser import SectionAwareConfigParser

LOCAL_CONFIG_FILE = "local.cfg"

DEFAULT_SECTION = "DEFAULT"

COMMON_CONFIG_PATH_PROP = "common_configs_relative_path"

COMMON_CONFIG_FILENAME_PROPS = {
    "common": "common_config_file",
    "metamorph": "metamorph_config_file",
    "teradata": "teradata_config_file",
    "aws_resources": "aws_resources_config_file",
    "airflow_connections": "airflow_connections_config_file",
    "gcp_resources": "gcp_resources_config_file",
}


class ConfigProvider:
    def __init__(self, section):
        self.__local_parser = SectionAwareConfigParser(section=DEFAULT_SECTION)
        self.__section = section

        # Read local config
        self.__local_parser.read(os.path.join(current_path, LOCAL_CONFIG_FILE))

        # Load common configs path
        common_configs_relative_path = self.__local_parser.get_property(
            COMMON_CONFIG_PATH_PROP
        )
        self.__common_configs_path = os.path.normpath(
            os.path.join(current_path, common_configs_relative_path)
        )

    # params:
    #   config_name - config name from COMMON_CONFIG_FILENAME_PROPS
    #
    # return: SectionAwareConfigParser instance with chosen preloaded common config
    def load(self, config_name: str) -> SectionAwareConfigParser:
        if config_name not in COMMON_CONFIG_FILENAME_PROPS:
            raise ValueError(
                f"Wrong value! Possible values: {list(COMMON_CONFIG_FILENAME_PROPS.keys())}"
            )
        config_filename = self.__local_parser.get_property(
            COMMON_CONFIG_FILENAME_PROPS[config_name]
        )
        config_parser = SectionAwareConfigParser(section=self.__section)
        config_parser.read(os.path.join(self.__common_configs_path, config_filename))
        return config_parser

    # params:
    #   config_path - path to the config to load
    #
    # return: SectionAwareConfigParser instance with config preloaded by provided path
    def load_from_path(self, config_path: str) -> SectionAwareConfigParser:
        config_parser = SectionAwareConfigParser(section=self.__section)
        config_parser.read(config_path)
        return config_parser
