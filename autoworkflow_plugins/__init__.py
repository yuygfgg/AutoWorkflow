"""
Community/optional plugins for AutoWorkflow.
Install optional third-party dependencies per plugin as needed.
"""

from .qbittorrent import QBittorrentPlugin, HasQBittorrent
from .bangumiMoe import BangumiMoePlugin, HasBangumiMoe
from .cloudreve import CloudrevePlugin, HasCloudreve

__all__ = [
    "QBittorrentPlugin",
    "HasQBittorrent",
    "BangumiMoePlugin",
    "HasBangumiMoe",
    "CloudrevePlugin",
    "HasCloudreve",
]
