import typing
import json
from datatime import datetime
from pathlib import Path

current_dir = Path(__file__).parent
config_path = current_dir / "config.json"
with open(config_path, "r") as f:
    station_config = json.load(f)

def get_station_data(station: str, timestamp: datetime) -> typing.Optional[dict]:
    history = station_config.get(station)
    
    if not history:
        return None

    for deployment in history:
        start = datetime.fromisoformat(deployment["start_time"].replace("Z", "+00:00"))
        end = datetime.fromisoformat(deployment["end_time"].replace("Z", "+00:00"))
        
        if start <= timestamp <= end:
            return deployment
            
    return None

def latitude(station: str, timestamp: datetime, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[float]:
    data = get_station_data(station, timestamp)
    return data["latitude"] if data else None


def longitude(station: str, timestamp: datetime, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[float]:
    data = get_station_data(station, timestamp)
    return data["longitude"] if data else None


def altitude(station: str, timestamp: datetime, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[float]:
    data = get_station_data(station, timestamp)
    return data["altitude"] if data else None


def country_code(station: str, timestamp: datetime, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    data = get_station_data(station, timestamp)
    return data["country_code"] if data else None


def subdivision(station: str, timestamp: datetime, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    data = get_station_data(station, timestamp)
    return data["subdivision"] if data else None


def name(station: str, timestamp: datetime, tags: typing.Optional[typing.Set[str]] = None) -> typing.Optional[str]:
    data = get_station_data(station, timestamp)
    if data:
        if tags and "radiation" in tags:
            return "CLAMPS3"
        return data["name"]
    return None