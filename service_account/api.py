import json
from typing import Dict, Optional, List

from google.oauth2 import service_account
import googleapiclient.discovery
from googleapiclient.errors import HttpError

from service_account.errors import (
    ExistedServiceAccountException,
    InvalidCredentialException
)

SCOPES = ['https://www.googleapis.com/auth/cloud-platform']
PREFIX = 'projects/-/serviceAccounts/'

class ServiceAccount:
    """ServiceAccount APIs"""
    def __init__(self, project_id: str, credential_path: Optional[str] = None) -> None:
        """Initialize ServiceAccount.

        :param project_id: project_id
        """
        try:
            self.project_id = project_id
            self.credentials = service_account.Credentials.from_service_account_file(
                filename=credential_path if credential_path else 'token.json',
                scopes=SCOPES
            )
            self.service = googleapiclient.discovery.build(
                'iam', 'v1', credentials=self.credentials
            )
        except Exception:
            raise InvalidCredentialException('Invalid credential information')

    def create_service_account(self, name: str, display_name: str) -> Optional[Dict]:
        """Create a new service account.

        :param name: service account name
        :param display_name: service account displayed name
        :return: newly created service account
        """
        try:
            new_service_account = self.service.projects().serviceAccounts().create(
                name='projects/' + self.project_id,
                body={
                    'accountId': name,
                    'serviceAccount': {
                        'displayName': display_name
                    }
                }
            ).execute()
            return new_service_account
        except Exception as exception:
            if isinstance(exception, HttpError):
                if exception.resp.status == 409:
                    raise ExistedServiceAccountException(f'{name} service account already exists.')
            raise exception

    def list_service_accounts(self) -> Optional[List]:
        """List all service accounts."""
        try:
            service_accounts = self.service.projects().serviceAccounts().list(
                name='projects/' + self.project_id
            ).execute()

            print(service_accounts)
            return service_accounts
        except Exception as exception:
            raise exception

    def rename_service_account(self, email: str, new_display_name: str):
        """Rename display-name of a service account.

        :param email: service account email_api
        :param new_display_name: new display-name
        :return:
        """
        try:
            resource = PREFIX + email
            serv_acc = self.service.projects().serviceAccounts().get(
                name=resource
            ).execute()
            serv_acc['displayName'] = new_display_name
            serv_acc = self.service.projects().serviceAccounts().update(
                name=resource,
                body=serv_acc
            ).service_account()
            return serv_acc
        except Exception as exception:
            raise exception

    def disable_service_account(self, email: str):
        """Disable a service account

        :param email: service account email_api
        :return:
        """
        try:
            self.service.projects().serviceAccounts().disable(
                name=PREFIX + email
            ).execute()
        except Exception as exception:
            raise exception

    def enable_service_account(self, email: str):
        """Enable a service account.

        :param email: service account email_api
        """
        try:
            response = self.service.projects().serviceAccounts().enable(
                name=PREFIX + email
            ).execute()
            return response
        except Exception as exception:
            raise exception

    def delete_service_account(self, email: str):
        """Delete a service account.

        :param email:
        """
        try:
            self.service.projects().serviceAccounts().delete(
                name=PREFIX + email
            ).execute()
        except Exception as exception:
            raise exception

    def generate_service_account_key(self, service_account_email: str, project_id: str):
        """Generate service account key.

        :param service_account_email: service account email
        :param project_id: project ID
        :return:
        """
        try:
            name = 'projects/' + project_id + '/serviceAccounts/' + service_account_email
            create_service_account_key_request_body = {
                "keyAlgorithm": "KEY_ALG_UNSPECIFIED",
                "privateKeyType": "TYPE_UNSPECIFIED"
            }

            response = self.service.projects().serviceAccounts().keys().create(
                name=name,
                body=create_service_account_key_request_body
            ).execute()
            with open('sa_keys/' + service_account_email.split('@')[0] + '.json', 'w') as f:
                f.write(json.dumps(response))

            return response
        except Exception as exception:
            raise exception
