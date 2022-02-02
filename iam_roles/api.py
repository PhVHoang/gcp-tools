import os
from typing import Optional

from googleapiclient import discovery
from google.oauth2 import service_account

SERVICE_ACCOUNT_CREDENTIAL_PATH_DEFAULT = os.getenv('SERVICE_ACCOUNT_CREDENTIAL_PATH_DEFAULT')


class IAM:
    """GCP IAM role APIs."""
    _SCOPES = ['https://www.googleapis.com/auth/cloud-platform']
    _SERVICE_NAME = 'iam'
    _API_VERSION = 'v1'

    def __init__(self) -> None:
        """Initialize."""
        try:
            self.credentials = service_account.Credentials.from_service_account_file(
                filename=SERVICE_ACCOUNT_CREDENTIAL_PATH_DEFAULT,
                scopes=self._SCOPES
            )
            self.service = discovery.build(
                self._SERVICE_NAME, self._API_VERSION, credentials=self.credentials
            )
        except Exception as exception:
            print("Failed to initialize IAM.")
            raise exception

    def create_role(self, name, project, title, permissions, stage,  description: Optional[str]):
        """Creates a role."""
        # pylint: disable=no-member
        try:
            role = self.service.projects().roles().create(
                parent='projects/' + project,
                body={
                    'roleId': name,
                    'role': {
                        'title': title,
                        'description': description,
                        'includedPermissions': permissions,
                        'stage': stage
                    }
                }).execute()

            return role
        except Exception as exception:
            raise exception

    def get_role(self, name):
        """Get a role."""
        try:
            role = self.service.roles().get(name=name).execute()
            return role
        except Exception as exception:
            raise exception

    def edit_role(
            self,
            name,
            project: Optional[str],
            title: Optional[str],
            description: Optional[str],
            permissions: Optional[str],
            stage: Optional[str]
    ):
        """Creates a role."""

        # pylint: disable=no-member
        try:
            role = self.service.projects().roles().patch(
                name='projects/' + project + '/roles/' + name,
                body={
                    'title': title,
                    'description': description,
                    'includedPermissions': permissions,
                    'stage': stage
                }).execute()

            return role
        except Exception as exception:
            raise exception

    def list_roles(self, project_id):
        """Lists roles."""

        # pylint: disable=no-member
        try:
            roles = self.service.roles().list(
                parent='projects/' + project_id).execute()['roles']
            return roles
        except Exception as exception:
            raise exception

    def disable_role(self, name, project):
        """Disables a role."""

        # pylint: disable=no-member
        try:
            role = self.service.projects().roles().patch(
                name='projects/' + project + '/roles/' + name,
                body={
                    'stage': 'DISABLED'
                }).execute()

            return role
        except Exception as exception:
            raise exception

    def delete_role(self, name, project):
        """Deletes a role."""

        # pylint: disable=no-member
        try:
            role = self.service.projects().roles().delete(
                name='projects/' + project + '/roles/' + name).execute()
            return role
        except Exception as exception:
            raise exception

    def undelete_roles(self, name, project):
        """Undeletes a role."""

        # pylint: disable=no-member
        try:
            role = self.service.projects().roles().patch(
                name='projects/' + project + '/roles/' + name,
                body={
                    'stage': 'DISABLED'
                }).execute()
            return role
        except Exception as exception:
            raise exception
