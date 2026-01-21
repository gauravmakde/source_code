from configparser import ConfigParser


class SectionAwareConfigParser:
    def __init__(self, section):
        self.__section = section
        self.__config_parser = ConfigParser()

    def read(self, path):
        self.__config_parser.read(path)

    def get_property(self, property_name):
        return self.__config_parser.get(self.__section, property_name)
