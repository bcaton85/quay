import pytest

from endpoints.api.superuser import SuperUserList, SuperUserManagement
from endpoints.api.test.shared import conduct_api_call
from endpoints.test.shared import client_with_identity
from test.fixtures import *


@pytest.mark.parametrize(
    "disabled",
    [
        (True),
        (False),
    ],
)
def test_list_all_users(disabled, client):
    with client_with_identity("devtable", client) as cl:
        params = {"disabled": disabled}
        result = conduct_api_call(cl, SuperUserList, "GET", params, None, 200).json
        assert len(result["users"])
        for user in result["users"]:
            if not disabled:
                assert user["enabled"]


@pytest.mark.parametrize(
    "query",
    [
        ("user"),
    ],
)
def test_query_users(query, client):
    with client_with_identity("devtable", client) as cl:
        params = {"query": query}
        result = conduct_api_call(cl, SuperUserList, "GET", params, None, 200).json
        assert len(result["users"])
        for user in result["users"]:
            assert query in user["username"]


@pytest.mark.parametrize(
    "field, direction",
    [
        ("username", "asc"),
        ("username", "desc"),
        ("email", "asc"),
        ("email", "desc"),
    ],
)
def test_sort_users(field, direction, client):
    with client_with_identity("devtable", client) as cl:
        params = {"sort": field, "direction": direction}
        result = conduct_api_call(cl, SuperUserList, "GET", params, None, 200).json
        assert len(result["users"])
        for i in range(1, len(result["users"])):
            if direction == "asc":
                assert result["users"][i - 1][field] <= result["users"][i][field]
            else:
                assert result["users"][i - 1][field] >= result["users"][i][field]


def test_change_install_user(client):
    with client_with_identity("devtable", client) as cl:
        params = {"username": "randomuser"}
        body = {"email": "new_email123@test.com"}
        result = conduct_api_call(cl, SuperUserManagement, "PUT", params, body, 200).json

        assert result["email"] == body["email"]
