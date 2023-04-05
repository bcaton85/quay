import pytest

from endpoints.api.superuser import SuperUserList, SuperUserManagement, SuperUserOrganizationList
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


def test_list_all_orgs(client):
    with client_with_identity("devtable", client) as cl:
        result = conduct_api_call(cl, SuperUserOrganizationList, "GET", None, None, 200).json
        assert len(result["organizations"])


@pytest.mark.parametrize(
    "query",
    [
        ("buynlarge"),
    ],
)
def test_query_orgs(query, client):
    with client_with_identity("devtable", client) as cl:
        params = {"query": query}
        result = conduct_api_call(cl, SuperUserOrganizationList, "GET", params, None, 200).json
        assert len(result["organizations"])
        for org in result["organizations"]:
            assert query in org["name"]


@pytest.mark.parametrize(
    "field, direction",
    [
        ("name", "asc"),
        ("name", "desc"),
        ("email", "asc"),
        ("email", "desc"),
    ],
)
def test_sort_orgs(field, direction, client):
    with client_with_identity("devtable", client) as cl:
        params = {"sort": field, "direction": direction}
        result = conduct_api_call(cl, SuperUserOrganizationList, "GET", params, None, 200).json
        assert len(result["organizations"])
        for i in range(1, len(result["organizations"])):
            if direction == "asc":
                assert result["organizations"][i - 1][field] <= result["organizations"][i][field]
            else:
                assert result["organizations"][i - 1][field] >= result["organizations"][i][field]


def test_change_install_user(client):
    with client_with_identity("devtable", client) as cl:
        params = {"username": "randomuser"}
        body = {"email": "new_email123@test.com"}
        result = conduct_api_call(cl, SuperUserManagement, "PUT", params, body, 200).json

        assert result["email"] == body["email"]
