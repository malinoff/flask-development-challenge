# coding=utf-8
"""
Exposes a simple HTTP API to search a users Gists via a regular expression.

Github provides the Gist service as a pastebin analog for sharing code and
other develpment artifacts.  See http://gist.github.com for details.  This
module implements a Flask server exposing two endpoints: a simple ping
endpoint to verify the server is up and responding and a search endpoint
providing a search across all public Gists for a given Github account.
"""

import re
import typing
from multiprocessing.pool import ThreadPool

import pydantic
import requests
import requests.adapters
from requests.packages.urllib3.util.retry import Retry
from flask import Flask, jsonify, request



# *The* app object
app = Flask(__name__)


@app.route("/ping")
def ping():
    """Provide a static response to a simple GET request."""
    return "pong"


def make_session(
        retries=3, backoff_factor=0.3, status_forcelist=(500, 502, 504)
    ):
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = requests.adapters.HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def gists_for_user(username):
    """Provides the list of gist metadata for a given user.

    This abstracts the /users/:username/gist endpoint from the Github API.
    See https://developer.github.com/v3/gists/#list-a-users-gists for
    more information.

    Args:
        username (string): the user to query gists for

    Returns:
        The dict parsed from the json response from the Github API.  See
        the above URL for details of the expected structure.
    """
    gists_url = 'https://api.github.com/users/{username}/gists'.format(
            username=username)
    session = make_session()
    page = 0
    # Let's not download the whole github, as our own paging
    # is not implemented
    while page <= 5:
        page += 1
        response = session.get(gists_url, params={"page": page})
        if response.status_code == requests.codes.not_found:
            raise UserNotFound
        if response.status_code == requests.codes.forbidden:
            # This should be the rate limiting reply check, omitted for brevity
            raise UnexpectedStatusCode
        if response.status_code != requests.codes.ok:
            # Here, In production, the exception should be sent to sentry
            raise UnexpectedStatusCode
        data = response.json()
        if not data:
            # If all pages were processed, an empty list is returned
            return
        try:
            yield pydantic.parse_obj_as(
                typing.List[GistApiResponseSchema], data
            )
        except pydantic.ValidationError as err:
            # Here, In production, the exception should be sent to sentry
            raise MalformedResponse


class UserNotFound(Exception):
    pass


class MalformedResponse(Exception):
    pass


class UnexpectedStatusCode(Exception):
    pass


@app.route("/api/v1/search", methods=['POST'])
def search():
    """Provides matches for a single pattern across a single users gists.

    Pulls down a list of all gists for a given user and then searches
    each gist for a given regular expression.

    Returns:
        A Flask Response object of type application/json.  The result
        object contains the list of matches along with a 'status' key
        indicating any failure conditions.
    """
    try:
        post_data = SearchRequestSchema.parse_obj(request.get_json())
    except pydantic.ValidationError as err:
        return {"status": "error", "message": err.json()}, 400

    username = post_data.username
    regex = re.compile(post_data.pattern)

    matches = []
    try:
        for page in gists_for_user(username):
            for gist in page:
                if _gist_matches(gist, regex):
                    matches.append(gist.html_url)
    except UserNotFound:
        message = f"User {username} not found"
        return {"status": "error", "message": message}, 404
    except (MalformedResponse, UnexpectedStatusCode) as exc:
        # In production, the message will be enriched with the debugging
        # information (like the sentry id)
        message = "Internal error"
        return {"status": "error", "message": message}, 500

    result = SearchResponseSchema(
        status="success",
        username=username,
        pattern=regex.pattern,
        matches=matches,
    )
    return result.dict(), 200


class SearchRequestSchema(pydantic.BaseModel):
    username: str
    pattern: str


class SearchResponseSchema(pydantic.BaseModel):
    status: str
    username: str
    pattern: str
    matches: typing.List[str]


class GistApiFileResponseSchema(pydantic.BaseModel):
    raw_url: str


class GistApiResponseSchema(pydantic.BaseModel):
    html_url: str
    files: typing.Dict[str, GistApiFileResponseSchema]


def _gist_matches(gist, regex):
    # This is a very simple sequential algorithm, which is obviously slow:
    # searching for a pattern in multiple texts can easily be parallelized,
    # but for the sake of bravity it is not implemented here.
    for gist_file in gist.files.values():
        for line in _fetch_file_lines(gist_file.raw_url):
            if regex.match(line):
                return True
    return False
                

def _fetch_file_lines(url):
    # According to the api,
    # https://docs.github.com/en/rest/reference/gists#truncation
    # for gists larger than 10 MB, GETting its url won't work,
    # it will be required to clone the gist via its git url.
    # Let's assume for the sake of brevity that all gists
    # we're interested in are less than 10 MB.
    session = make_session()
    response = session.get(url, stream=True)
    for line in response.iter_lines(decode_unicode=True):
        yield line


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=9876)
