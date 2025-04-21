# -*- coding: utf-8 -*-

from configparser import ConfigParser

from ws_streamer.utilities import system_tools


class Read_Configuration:
    """
    # Read .env file
    """

    def __init__(self):
        self.params = None
        self.conn = None

    def config(
        self, 
        filename: str, 
        section: str,
        ) -> dict:
        
        # create parser
        parser = ConfigParser()

        # read file config
        parser.read(filename)

        # prepare place holder for file config read result
        parameters = {}

        if self.params is None:
            # check file section
            if parser.has_section(section):
                params = parser.items(section)
        
                for param in params:
                    parameters[param[0]] = param[1]

            # if section is not provided
            else:
                raise Exception(
                    "Section {0} not found in the {1} file".format(section, filename)
                )

        return parameters


def main_dotenv(
    header: str = "None", 
    config_path: str = ".env",
    ) -> dict:
    
    """
    # Read .env file"""

    # Initialize credentials to None
    credentials = None

    try:
        # Set the filename
        # filename = ".env"
#        config_path = system_tools.provide_path_for_file(filename)

        # Create a Read_Configuration object
        Connection = Read_Configuration()

        credentials = Connection.config(config_path, header)

    # to accomodate transition phase. Will be deleted
    except:
        import os
        from os.path import dirname, join

        from dotenv import load_dotenv

        dotenv_path = join(dirname(__file__), ".env")
        load_dotenv(dotenv_path)

        # github env
        credentials = os.environ
        # log.info (credentials)

    return credentials
