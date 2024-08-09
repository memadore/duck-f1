import json
from datetime import datetime
from hashlib import sha1
from urllib.request import urlopen

import yaml

# source: https://github.com/theOehrly/f1schedule/tree/master
SEASON_CALENDARS = [
    "https://github.com/theOehrly/f1schedule/raw/master/schedule_2018.json",
    "https://github.com/theOehrly/f1schedule/raw/master/schedule_2019.json",
    "https://github.com/theOehrly/f1schedule/raw/master/schedule_2020.json",
    "https://github.com/theOehrly/f1schedule/raw/master/schedule_2021.json",
    "https://github.com/theOehrly/f1schedule/raw/master/schedule_2022.json",
    "https://github.com/theOehrly/f1schedule/raw/master/schedule_2023.json",
    "https://github.com/theOehrly/f1schedule/raw/master/schedule_2024.json",
]

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"

events = []
for url in SEASON_CALENDARS:
    with urlopen(url) as resp:
        data = json.loads(resp.read())

    for key, value in data["event_name"].items():
        event_data = {
            "date": datetime.strptime(data["event_date"][key], DATETIME_FORMAT),
            "name": value.strip(),
            "official_event_name": data["official_event_name"][key],
            "location": data["location"][key].strip(),
            "country": data["country"][key].strip(),
            "round_number": data["round_number"][key],
            "weekend_format": data["event_format"][key].strip(),
            "gmt_offset": data["gmt_offset"][key].strip().replace("'", ""),
            "sessions": [],
        }

        event_sha = sha1(json.dumps(event_data, default=str).encode()).hexdigest()
        event_data.update({"sha": event_sha[:8]})

        for i in range(1, 6):
            session_key = f"session{i}"
            session_date = f"session{i}_date"

            if data[session_date][key] is None or len(data[session_date][key]) == 0:
                continue

            session_data = {
                "date": datetime.strptime(data[session_date][key], DATETIME_FORMAT),
                "name": data[session_key][key].strip(),
                "type": data[session_key][key].strip().replace(" ", "_").lower(),
            }

            session_unique = {**session_data, "event_key": event_sha}

            session_sha = sha1(
                json.dumps(session_unique, default=str).encode()
            ).hexdigest()
            session_data.update({"sha": session_sha[:8]})

            event_data["sessions"].append(session_data)

        events.append(event_data)

with open(
    "./src/duck_f1/pipelines/assets/live_timing/values.yaml", "r", encoding="utf-8"
) as file:
    file_data = yaml.safe_load(file)

with open(
    "./src/duck_f1/pipelines/assets/live_timing/values.yaml", "w", encoding="utf-8"
) as file:
    file_data["events"] = events
    yaml.safe_dump(file_data, file)
