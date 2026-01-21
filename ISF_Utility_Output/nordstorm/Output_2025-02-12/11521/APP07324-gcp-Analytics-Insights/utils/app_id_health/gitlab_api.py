import requests


class GitLab:
    """
    Find who was engaged in Standards MR work in a given month.

    Engagement is defined as anyone who:
       - created a MR
       - commented or had discussion on a MR
       - upvoted a MR

    """

    def __init__(self, ACCESS_TOKEN):
        """
        Parameters
        ----------
        ACCESS_TOKEN: str
            This is a personal access token from GitLab and authorizes the API
        """
        self.ACCESS_TOKEN = ACCESS_TOKEN

    def check_token(self):
        """
        Send a simple endpoint to the GitLab API to check the connection.
        """
        endpoint = "https://git.jwn.app/api/v4/version"
        api_auth = {"authorization": f"Bearer {self.ACCESS_TOKEN}"}
        response = requests.get("https://git.jwn.app/api/v4/version", headers=api_auth)
        if response.status_code != 200:
            print("*** There's a problem with the access token. ***")

    def send_endpoint(self, endpoint):
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
        return_length = 100
        search_results_list = []

        while return_length == 100:
            if "?" in endpoint:
                search_results = requests.get(
                    endpoint + f"&per_page=100&page={page_num}", headers=api_auth
                )
            else:
                search_results = requests.get(
                    endpoint + f"?per_page=100&page={page_num}", headers=api_auth
                )

            if isinstance(search_results.json(), dict):
                return search_results.json()

            search_results_list += search_results.json()
            return_length = len(search_results.json())
            page_num += 1

        return search_results_list
