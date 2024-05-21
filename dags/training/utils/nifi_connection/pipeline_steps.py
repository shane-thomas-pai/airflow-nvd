import logging
from time import sleep

from training.utils.nifi_connection.update_processor import update_process_group


def trigger_nifi_flow():
    update_process_group("5cda0d0e-018f-1000-ffff-ffff96b59b62", "RUNNING")
    sleep(15)
    update_process_group("5cda0d0e-018f-1000-ffff-ffff96b59b62", "STOPPED")


def log_status():
    logging.info("Executed NiFi flow successfully.")


if __name__ == "__main__":
    trigger_nifi_flow()
