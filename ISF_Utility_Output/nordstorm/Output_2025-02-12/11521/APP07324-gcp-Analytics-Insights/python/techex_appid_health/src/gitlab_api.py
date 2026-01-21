from typing import Dict, List

import requests


class GitLab:
    """
    Get info from the GitLab API.
    This class just connects with the API and sends/receives the response
    """

    def __init__(self, ACCESS_TOKEN: str):
        """
        Parameters
        ----------
        ACCESS_TOKEN: str
            To access the GitLab API, you authenticate with your Personal Access Token (PAT).
            You can find (or create) these in GitLab in your profile under Access Tokens
            (https://git.jwn.app/-/profile/personal_access_tokens).  Note that this PAT needs
            to be created with the api and read_api boxes checked to allow access to the API.
        """
        self.ACCESS_TOKEN = ACCESS_TOKEN

    def check_token(self):
        """
        Send a simple endpoint to the GitLab API to check the connection.
        """
        # endpoint to find the installed version of GitLab (a simple request)
        endpoint = "https://git.jwn.app/api/v4/version"
        api_auth = {"authorization": f"Bearer {self.ACCESS_TOKEN}"}
        response = requests.get("https://git.jwn.app/api/v4/version", headers=api_auth)

        # if you don't get a 200 response (it worked!) then print a message
        if response.status_code != 200:
            print("*** There's a problem with the access token. ***")

    def send_endpoint(self, endpoint: str) -> List[Dict[str, str]]:
        """
        Call the API and return the results.

        Note that the API has a max return length of 100.  So to get items beyond
        100 you send a second page. This function returns as many pages as needed
        to get all the results

        Parameter
        ---------
        endpoint: str
            The endpoint for the API.
            For example "https://git.jwn.app/api/v4/projects/7476/merge_requests"

        Return
        ------
        search_results_list: json
            The result of the API call
        """
        api_auth = {"authorization": f"Bearer {self.ACCESS_TOKEN}"}

        page_num = 1
        return_length = 100  # max number that API returns at a time
        search_results_list = []

        # The api only returns 100 items at a time and we want to capture ALL the items.
        # So loop through sets of 100 until we get them all.
        while return_length == 100:
            if "?" in endpoint:
                search_results = requests.get(
                    endpoint + f"&per_page=100&page={page_num}", headers=api_auth
                )
            else:
                search_results = requests.get(
                    endpoint + f"?per_page=100&page={page_num}", headers=api_auth
                )

            if search_results.status_code != 200:
                print("GitLab API call not sucessful")
                print(endpoint)

            # if API returns a single Dict instead of a List of items, return it
            if isinstance(search_results.json(), dict):
                return search_results.json()

            # add this batch of 100 to the list and move to the next batch if there's more
            search_results_list += search_results.json()
            return_length = len(search_results.json())
            page_num += 1

        return search_results_list
