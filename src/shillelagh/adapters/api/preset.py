"""
Simple adapter for the Preset API (https://preset.io/).

This is a derivation of the generic JSON adapter that handles Preset auth.
"""

from typing import Any, Optional, cast

import requests
from yarl import URL

from shillelagh.adapters.api.generic_json import GenericJSONAPI


def get_jwt_token(access_token: str, access_secret: str) -> str:
    """
    Get JWT token from access token and access secret.
    """
    response = requests.post(
        "https://api.app.preset.io/v1/auth/",
        json={"name": access_token, "secret": access_secret},
        headers={"Content-Type": "application/json"},
        timeout=60,
    )
    response.raise_for_status()
    payload = response.json()
    return cast(str, payload["payload"]["access_token"])


class PresetAPI(GenericJSONAPI):
    """
    Custom JSON adapter that handlers Preset auth.
    """

    default_path = "$.payload[*]"
    cache_name = "preset_cache"

    @classmethod
    def supports(cls, uri: str, fast: bool = True, **kwargs: Any) -> Optional[bool]:
        parsed = URL(uri)
        return parsed.scheme in ("http", "https") and (
            parsed.host == "preset.io" or parsed.host.endswith(".preset.io")
        )

    def __init__(
        self,
        uri: str,
        path: Optional[str] = None,
        access_token: Optional[str] = None,
        access_secret: Optional[str] = None,
    ):
        if access_token is None or access_secret is None:
            raise ValueError("access_token and access_secret must be provided")

        jwt_token = get_jwt_token(access_token, access_secret)
        request_headers = {"Authorization": f"Bearer {jwt_token}"}
        super().__init__(uri, path=path, request_headers=request_headers)
