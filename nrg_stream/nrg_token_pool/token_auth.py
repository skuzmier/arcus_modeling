import json
import datetime
import requests
import os


class TokenAuth:
    """ """

    def __init__(self, username=None, password=None, token=None):
        """ """
        self.username = username
        self.password = password
        self._token = token
        self.server = "api.nrgstream.com"
        self.token_path = "/api/security/token"
        self.release_path = "/api/ReleaseToken"
        self.token_payload = None
        self.expiry = None
        self.expires_in_seconds = None
        self._token = None
        self._setup()

    def _setup(self):
        """validate username and password were passed at instantiation
        it not search for them in env vars, then in a credentials.txt file, then in a credentials.json file
        """
        if self.username is None or self.password is None:
            if (
                "NRGSTREAM_USERNAME" in os.environ
                and "NRGSTREAM_PASSWORD" in os.environ
            ):
                self.username = os.environ["NRGSTREAM_USERNAME"]
                self.password = os.environ["NRGSTREAM_PASSWORD"]
            elif os.path.exists("credentials.txt"):
                with open("credentials.txt", "r") as f:
                    self.username = f.readline().split(",")[0]
                    self.password = f.readline().split(",")[1]
            elif os.path.exists("credentials.json"):
                with open("credentials.json", "r") as f:
                    self.username = json.load(f)["username"]
                    self.password = json.load(f)["password"]
            else:
                raise ValueError(
                    "username and password must be passed at instantiation or in env vars or in a credentials.txt or credentials.json file"
                )

        self.token_payload = (
            f"grant_type=password&username={self.username}&password={self.password}"
        )

    @property
    def token(self):
        """ """
        if self._token is None:
            self._get_token()
        elif self.expiry < datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=5):
            self._get_token()
        return self._token

    def _get_token(self):
        """get the token from NRG Stream using requests package"""
        headers = {"Content-type": "application/x-www-form-urlencoded"}
        response = requests.post(
            f"https://{self.server}{self.token_path}",
            data=self.token_payload,
            headers=headers,
        )
        if response.status_code == 200:
            self._token = response.json()["access_token"]
            self.expiry = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
                seconds=response.json()["expires_in"]
            )
            self.expires_in_seconds = int(response.json()["expires_in"])
        else:
            raise ValueError(
                f"failed to get token from NRG Stream: {response.status_code} - {response.reason}"
            )

    def release_token(self):
        """ """
        if self._token is None:
            return True
        if self.expiry < datetime.datetime.now(datetime.timezone.utc):
            return True
        headers = {}
        headers["Authorization"] = f"Bearer {self.token}"
        response = requests.delete(
            f"https://{self.server}{self.release_path}", headers=headers
        )
        if response.status_code == 200:
            self._token = None
            self.expiry = None
        else:
            raise ValueError(
                f"failed to release token from NRG Stream: {response.status_code} - {response.reason}"
            )


class BearerAuth(requests.auth.AuthBase):
    """Attaches HTTP Bearer Authentication to the given Request object."""

    def __init__(self, token):
        """ """
        self.token = token

    def __call__(self, r):
        """modify the request to include the bearer token"""
        r.headers["Authorization"] = f"Bearer {self.token}"
        return r
