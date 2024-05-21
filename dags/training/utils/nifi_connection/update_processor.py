import json
import logging

import requests


def update_process_group(process_group_id, new_state):
    put_dict = {
        "id": process_group_id,
        "state": new_state
    }
    logging.info("Starting API call...")
    payload = json.dumps(put_dict).encode("utf8")
    header = {
        "Content-Type": "application/json"
    }
    response = requests.put(
        f"http://host.docker.internal:55673/nifi-api/flow/process-groups/{process_group_id}",
        headers=header,
        data=payload,
    )
    print(response.status_code)
