from test.fixtures import *
from unittest.mock import MagicMock, patch

import pytest, json

from data.model.autoprune import *
from data.model.user import get_user
from data.model import InvalidUsernameException, NamespacePolicyAlreadyExists
from data.model.organization import create_organization


ORG1_NAME = "org1"
ORG2_NAME = "org2"

class TestNameSpaceAutoprune:

    @pytest.fixture(autouse=True)
    def setup(self, initialized_db):
        user = get_user("devtable")
        self.org1 = create_organization(ORG1_NAME, f"{ORG1_NAME}@devtable.com", user)
        self.org2 = create_organization(ORG2_NAME, f"{ORG2_NAME}@devtable.com", user)

    def test_policy_creation_without_task(self):
        # policy based on tag count
        number_of_tags_policy =  {"method":"number_of_tags", "value": 10}
        new_policy1 = create_namespace_autoprune_policy(ORG1_NAME, number_of_tags_policy, create_task=False)
        assert new_policy1.namespace.id == self.org1.id
        assert json.loads(new_policy1.policy) == number_of_tags_policy

        # policy based on tag creation date
        create_date_policy =  {"method":"creation_date", "value": 1695668761}
        new_policy2 = create_namespace_autoprune_policy(ORG2_NAME, create_date_policy, create_task=False)
        assert new_policy2.namespace.id == self.org2.id
        assert json.loads(new_policy2.policy) == create_date_policy
    
    def test_policy_creation_with_task(self):
        number_of_tags_policy =  {"method":"number_of_tags", "value": 10}
        new_policy = create_namespace_autoprune_policy(ORG1_NAME, number_of_tags_policy, create_task=True)
        assert new_policy.namespace.id == self.org1.id
        assert namespace_has_autoprune_task(self.org1.id) is True
    
    def test_policy_creation_with_incorrect_org(self):
        number_of_tags_policy =  {"method":"number_of_tags", "value": 10}
        with pytest.raises(InvalidUsernameException) as excerror:
            create_namespace_autoprune_policy("non-existant org", number_of_tags_policy, create_task=True)
        assert str(excerror.value) == "Invalid namespace provided: non-existant org"
    
    def test_policy_creation_for_org_with_policy(self):
        number_of_tags_policy =  {"method":"number_of_tags", "value": 10}
        new_policy1 = create_namespace_autoprune_policy(ORG1_NAME, number_of_tags_policy, create_task=False)
        with pytest.raises(NamespacePolicyAlreadyExists) as excerror:
            create_namespace_autoprune_policy(ORG1_NAME, number_of_tags_policy, create_task=True)
        assert str(excerror.value) == "Policy for this namespace already exists, delete existing to create new policy"

    def test_get_policies_by_orgname(self):
        number_of_tags_policy =  {"method":"number_of_tags", "value": 10}
        create_namespace_autoprune_policy(ORG1_NAME, number_of_tags_policy, create_task=False)
        policies_org1 = get_namespace_autoprune_policies_by_orgname(ORG1_NAME)
        assert len(policies_org1) == 1
        assert policies_org1[0].namespace_id == self.org1.id

        policies_org2 = get_namespace_autoprune_policies_by_orgname(ORG2_NAME)
        assert len(policies_org2) == 0
    
    def test_get_policies_by_namespace_id(self):
        create_date_policy =  {"method":"creation_date", "value": 1695668761}
        create_namespace_autoprune_policy(ORG1_NAME, create_date_policy, create_task=False)
        policies_org1 = get_namespace_autoprune_policies_by_id(self.org1.id)
        assert len(policies_org1) == 1
        assert policies_org1[0].namespace_id == self.org1.id

        policies_org2 = get_namespace_autoprune_policies_by_orgname(ORG2_NAME)
        assert len(policies_org2) == 0
