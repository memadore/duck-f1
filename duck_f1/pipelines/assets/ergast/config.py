from typing import List

from pydantic import BaseModel


class ErgastAsset(BaseModel):
    file: str
    asset_name: str


class ErgastConfig(BaseModel):
    url: str
    assets: List[ErgastAsset]
