
import os
import glob
import re

from Util.function_use import fetch_details_sql_file

# SqlFileLst = glob.glob(os.path.join("Input", '**', '*.sql'),recursive=True)


# for file in SqlFileLst:
#
#     location = os.path.dirname(file)
#     file_name = os.path.basename(file)
#     fetch_details_sql_file(file)
#

import re

def is_palindrome(s):
    # Remove non-alphanumeric characters and convert to lowercase
    cleaned = re.sub(r'[^A-Za-z0-9]', '', s).lower()
    return cleaned == cleaned[::-1]

# Example usage
print(is_palindrome("A man, a plan, a canal: Panama"))  # True
print(is_palindrome("hello"))  # False