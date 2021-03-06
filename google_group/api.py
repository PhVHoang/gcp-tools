import os
from typing import Optional, List
from urllib.parse import urlencode

from googleapiclient.errors import HttpError
import googleapiclient.discovery
from google.oauth2 import service_account

from google_group.errors import (
    ExistedMembershipException,
    MemberDoesNotExistException,
)

SERVICE_ACCOUNT_CREDENTIAL_PATH_DEFAULT = os.getenv('SERVICE_ACCOUNT_CREDENTIAL_PATH_DEFAULT')


class Groups:
    """Google Groups API interactions."""
    _SCOPES = [
        'https://www.googleapis.com/auth/cloud-identity.groups',
        'https://www.googleapis.com/auth/admin.directory.user',
        'https://www.googleapis.com/auth/admin.directory.group.member',
        'https://www.googleapis.com/auth/admin.directory.group.readonly'
    ]
    _CLOUD_IDENTITY_SERVICE_NAME = 'cloudidentity'
    _CLOUD_IDENTITY_API_VERSION = 'v1'

    def __init__(self) -> None:
        """Initialize service."""
        assert SERVICE_ACCOUNT_CREDENTIAL_PATH_DEFAULT, "Service account path must be provided."
        self.cloud_identity_service = self.__create_service(
            service_name=self._CLOUD_IDENTITY_SERVICE_NAME,
            service_version=self._CLOUD_IDENTITY_API_VERSION,
            scopes=self._SCOPES,
            credentials_path=SERVICE_ACCOUNT_CREDENTIAL_PATH_DEFAULT
        )

    @staticmethod
    def __create_service(
            service_name: str,
            service_version: str,
            credentials_path: str,
            scopes: Optional[List[str]]
    ):
        """Create a google service. (

        :param service_name: service name
        :param service_version: api version
        :param credentials_path: path to credential token file (json file)
        :param scopes: scopes
        :return: A google service instance
        """
        try:
            credentials = service_account.Credentials.from_service_account_file(
                filename=credentials_path,
                scopes=scopes
            )
            service = googleapiclient.discovery.build(
                service_name,
                service_version,
                credentials=credentials
            )
            return service
        except Exception as exception:
            raise exception

    def create_group(self, *, group_id, group_display_name, customer_id: str,
                     group_description: Optional[str] = None):
        """Create a new group"""
        group_key = {"id": group_id}
        group = {
            "parent": "customers/" + customer_id,
            "description": group_description,
            "displayName": group_display_name,
            "groupKey": group_key,
            # Set the label to specify creation of a Google Group.
            "labels": {
                "cloudidentity.googleapis.com/groups.discussion_forum": ""
            }
        }
        try:
            request = self.cloud_identity_service.groups().create(body=group)
            request.uri += "&initialGroupConfig=WITH_INITIAL_OWNER"
            request.execute()
        except Exception as exception:
            raise exception

    def list_google_group_memberships(self, group_id):
        """List group memberships by using groups.memberships.list API

        :param group_id:
        :return:
        """
        param = "&groupKey.id=" + group_id
        try:
            lookup_group_name_request = self.cloud_identity_service.groups().lookup()
            lookup_group_name_request.uri += param
            lookup_group_name_response = lookup_group_name_request.execute()
            group_name = lookup_group_name_response.get("name")
            # List memberships
            response = self.cloud_identity_service.groups().memberships().list(
                parent=group_name,
                pageSize=1000
            ).execute()
            return response
        except Exception as exception:
            raise exception

    def search_transitive_groups(self, member, page_size) -> Optional[List]:
        """Search all groups memberships of a member.

        :param member: member email
        :param page_size: page size
        :return:
        """
        try:
            group_list = []
            next_page_token = ''
            while True:
                query_params = urlencode(
                    {
                        "query": "member_key_id == '{}' "
                                 "&& 'cloudidentity.googleapis.com/groups.discussion_forum' in labels".format(member),
                        "page_size": page_size,
                        "page_token": next_page_token
                    }
                )
                request = self.cloud_identity_service.groups().memberships().searchTransitiveGroups(parent='groups/-')
                request.uri += "&" + query_params
                response = request.execute()

                if 'memberships' in response:
                    group_list += response['memberships']

                if 'nextPageToken' in response:
                    next_page_token = response['nextPageToken']
                else:
                    next_page_token = ''

                if len(next_page_token) == 0:
                    break
            return group_list

        except Exception as exception:
            raise exception

    def create_google_group_membership(self, group_key: str, member_key: str, role_name: Optional[str] = 'MEMBER'):
        """Add member to group.

        :param group_key: group id
        :param member_key: member email
        :param role_name: role of this member in group (MEMBER - MANAGER - OWNER), default is MEMBER
        :return: response
        """
        param = "&groupKey.id=" + group_key
        try:
            lookup_group_name_request = self.cloud_identity_service.groups().lookup()
            lookup_group_name_request.uri += param
            # Given a group ID and namespace, retrieve the ID for parent group
            lookup_group_name_response = lookup_group_name_request.execute()
            group_name = lookup_group_name_response.get("name")
            # Create a membership object with a memberKey and a single role of type MEMBER
            membership = {
                "preferredMemberKey": {
                    "id": member_key
                },
                "roles": {
                    "name": role_name,
                }
            }
            # Create a membership using the ID for the parent group and a membership object
            response = self.cloud_identity_service.groups().memberships().create(parent=group_name,
                                                                                 body=membership).execute()
            return response
        except Exception as exception:
            if isinstance(exception, HttpError):
                if exception.resp.status == 409:
                    raise ExistedMembershipException(f'{member_key} already exists on {group_key}')
                if exception.resp.status == 403:
                    raise MemberDoesNotExistException(f'{member_key} does not exist')
            raise exception

    def remove_membership(self, *, group_id: str, membership_id: str):
        """Remove a user from group.

        :param group_id: group email
        :param membership_id: user email
        :return:
        """
        try:
            request = self.cloud_identity_service.groups().memberships().delete(
                name=f'groups/{group_id}/memberships/{membership_id}'
            )
            response = request.execute()
            print(f'Successfully removed {membership_id} from {group_id}')
            return response
        except Exception as exception:
            raise exception

    def list_groups(self):
        """List all google groups.

        :return: a list of group names
        """
        try:
            next_page_token = ''
            group_list = []
            while True:
                query_params = urlencode({
                    'page_size': 50,
                    'page_token': next_page_token
                })
                request = self.cloud_identity_service.groups().list(parent='customers/C023nd09l')
                request.uri += '&' + query_params
                response = request.execute()
                if 'groups' in response and len(response['groups']) > 0:
                    group_list.extend([
                        (info['name'].split('/')[1], info['groupKey']['id']) for info in response['groups']
                    ])

                if 'nextPageToken' in response:
                    next_page_token = response['nextPageToken']
                else:
                    next_page_token = ''

                if len(next_page_token) == 0:
                    break
            return group_list
        except Exception as exception:
            raise exception

    def remove_memberships_from_all_groups(self, membership_id: str, group_ids: List[str]):
        """Revoke all memberships of a user.

        :param membership_id: membership id
        :param group_ids: List of group ids where this membership_id is a member.
        :return:
        """

        for group_id in group_ids:
            try:
                self.remove_membership(group_id=group_id, membership_id=membership_id)
            except Exception as exception:
                raise exception

    def restore_memberships(self, member_key: str, group_keys: List[str]):
        """Restore all memberships of membership_id

        :param member_key: user email
        :param group_keys: group key list
        :return:
        """
        for group_key in group_keys:
            try:
                self.create_google_group_membership(group_key=group_key, member_key=member_key)
            except Exception as exception:
                raise exception

    def delete_group(self, group_id: str):
        """Delete a google group

        :param group_id: group id
        :return:
        """
        try:
            request = self.cloud_identity_service.groups().delete(
                name=f'groups/{group_id}'
            )
            response = request.execute()
            return response
        except Exception as exception:
            raise exception
