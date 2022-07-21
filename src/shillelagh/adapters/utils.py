import google.auth
import google.oauth2.credentials
import google.oauth2.service_account
from google.auth.credentials import Credentials
from typing import Any, Dict, Iterator, List, Optional, Tuple, Type

def get_credentials(
    access_token: Optional[str] = None,
    service_account_file: Optional[str] = None,
    service_account_info: Optional[Dict[str, Any]] = None,
    subject: Optional[str] = None,
    app_default_credentials: Optional[bool] = False,
    scopes:List[str] = []
) -> Optional[Credentials]:
    """
    Return a set of credentials.

    The user can provide either an OAuth token directly, the location of a service
    account file, or the contents of the service account directly. When passing
    credentials from a service account the user can also specify a "subject", used
    to impersonate a given user. Application default credentials can also be used
    from the environment in lieu of directly specifying a token or service account
    information.
    """
    if access_token:
        return google.oauth2.credentials.Credentials(access_token)

    if service_account_file:
        return google.oauth2.service_account.Credentials.from_service_account_file(
            service_account_file,
            scopes=scopes,
            subject=subject,
        )

    if service_account_info:
        return google.oauth2.service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=scopes,
            subject=subject,
        )

    if app_default_credentials:
        return google.auth.default(scopes=scopes)[0]

    return None