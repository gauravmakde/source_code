import requests

# Set the base URL of your MicroStrategy environment
base_url = "https://express.microstrategy.com/MicroStrategyLibrary/api"

# Set the username and password for authentication
username = "iqbal.shaikh@datametica.com"
password = "Nikbal@1121"


# Construct the login endpoint URL
login_url = f"{base_url}/auth/login"

# Set the request headers
headers = {
    "Content-Type": "application/json",
    "Accept": "application/json"
}

# Set the request payload with the username and password
payload = {
    "username": username,
    "password": password,
    'loginMode': 1
}

# Send the login request
response = requests.post(login_url, headers=headers, json=payload)

# Check if the login was successful
if response.status_code == 200:
    # Get the X-MicroStrategy-AuthToken from the response headers
    auth_token = response.headers.get("X-MicroStrategy-AuthToken")
    print(f"X-MicroStrategy-AuthToken: {auth_token}")
else:
    print("Login failed. Please check your credentials.")
