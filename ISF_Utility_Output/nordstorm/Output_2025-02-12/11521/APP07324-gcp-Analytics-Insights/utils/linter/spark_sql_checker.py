import re
from base_sql_checker import BaseSQLChecker


class SparkSQLChecker(BaseSQLChecker):

    def __init__(self, phrases):
        super().__init__(phrases)

    def check_file(self, sql_file: str) -> None:
        """
            Check a SQL file for compliance with certain rules.

            Args:
                sql_file (str): The path to the SQL file to check.

            Raises:
                Exception: If the SQL file does not start with a 'set query_band' statement, or if a statement does not comply
                with the rules checked by the sql_phrase_checker method.
            """
        parsed = super().parse_sql_file(sql_file)
        for statement in parsed:
            clean_statement = re.sub('\s+', ' ', statement.lower())
            for phrase in self.phrases:
                self.sql_phrase_checker(phrase, clean_statement)

    def sql_phrase_checker(self, phrase: str, clean_statement: str) -> None:
        """
        Check a SQL statement for compliance with certain rules.

        Args:
            phrase (str): The SQL command or keyword to check for.
            clean_statement (str): The SQL statement to check.

        Raises:
            Exception: If the SQL statement does not comply with the rules checked by this function.
        """
        # Implement specific checks for Spark SQL
        if "s3://ace-etl" in clean_statement and ("create temporary view" not in clean_statement
                                                  and "create or replace temp view" not in clean_statement
                                                  and "create or replace temporary view" not in clean_statement):
            print(clean_statement)
            raise Exception(
                "Error: You cannot create Hive tables directly against `s3://ace-etl` - please use {hive_schema}")
        if phrase.lower() in clean_statement:
            clean_statement = (clean_statement[clean_statement.find(phrase):])
            clean_statement_split = clean_statement.split(" ")
            element = len(phrase.split(" "))
            if "if" in clean_statement_split:
                clean_statement_split.remove("if")
            if "exists" in clean_statement_split:
                clean_statement_split.remove("exists")
            if "not" in clean_statement_split:
                clean_statement_split.remove("not")
            if "ace_etl" in clean_statement_split[element]:
                print(clean_statement_split)
                raise Exception(f"Error: You cannot execute {phrase.upper()} on a Hive Table Directly")


