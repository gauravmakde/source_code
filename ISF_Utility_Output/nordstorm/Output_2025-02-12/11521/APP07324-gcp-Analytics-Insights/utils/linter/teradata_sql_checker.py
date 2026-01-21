import re
from base_sql_checker import BaseSQLChecker


class TeradataSQLChecker(BaseSQLChecker):

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
        starts_with_qb = False
        for statement in parsed:
            clean_statement = re.sub('\s+', ' ', statement.lower())
            if not starts_with_qb:
                if 'set query_band' in clean_statement.lower():
                    starts_with_qb = True
                else:
                    raise Exception(
                        f"Error - your Teradata SQL file MUST start with a Query Band - got {clean_statement}")
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
        # Handle references to T3 tables
        if "t3dl_" in clean_statement and 't3dl_ace_corp.znxy_liveramp_events' not in clean_statement \
                and 't3dl_paymnt_loyalty.credit_close_dts_20230719_v3' not in clean_statement \
                and 't3dl_paymnt_loyalty.creditloyaltysync' not in clean_statement:  # add exception for credit loyalty issues from NAP Engineering
            raise Exception(f"Error: You cannot reference a T3 object in an ISF Pipeline - got {clean_statement}")
        if phrase.lower() in clean_statement:
            clean_statement = (clean_statement[clean_statement.find(phrase):])
            clean_statement_split = clean_statement.split(" ")
            # CALL SYS_MGMT REQUIRES SPECIAL HANDLING
            if phrase.lower() == "call":
                # attenuate for when the literal word "call" is used
                if "sys_mgmt" not in clean_statement_split[1]:
                    pass
                else:
                    for item in clean_statement_split:
                        if "t2dl_" in item and "t2dl_das_bie_dev" not in item:
                            print(clean_statement_split)
                            raise Exception(
                                f"Error: You cannot execute a CALL SYS_MGMT procedure directly against a T2")
            else:
                # The [xth] element i.e. (DELETE FROM X / DROP TABLE X / CREATE MULTISET TABLE X)
                element = len(phrase.split(" "))
                if "if" in clean_statement_split:
                    clean_statement_split.remove("if")
                if "exists" in clean_statement_split:
                    clean_statement_split.remove("exists")
                if "not" in clean_statement_split:
                    clean_statement_split.remove("not")
                if "t2dl_" in clean_statement_split[element] and "t2dl_das_bie_dev" not in clean_statement_split[
                    element]:
                    # Add exception for TRUST which does NOT test in BIE_DEV
                    if "t2dl_das_trust_emp" not in clean_statement_split[element] and "{t2_test}" not in \
                            clean_statement_split[element]:
                        print(clean_statement_split)
                        raise Exception(f"Error: You cannot execute {phrase.upper()} on a T2DL_ / Hive Table Directly")
