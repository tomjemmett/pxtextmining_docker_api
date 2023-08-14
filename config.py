"""Configuration values"""
# pylint: disable=line-too-long

import os

import dotenv

dotenv.load_dotenv()

SUBSCRIPTION_ID = os.environ["SUBSCRIPTION_ID"]
CONTAINER_IMAGE = os.environ["CONTAINER_IMAGE"]
AZURE_LOCATION = os.environ["AZURE_LOCATION"]

CONTAINER_MEMORY = os.environ["CONTAINER_MEMORY"]
CONTAINER_CPU = os.environ["CONTAINER_CPU"]

STORAGE_ACCOUNT = os.environ["STORAGE_ACCOUNT"]
STORAGE_ENDPOINT = f"https://{STORAGE_ACCOUNT}.file.core.windows.net/"
STORAGE_KEY = os.environ["STORAGE_KEY"]

AUTO_DELETE_COMPLETED_CONTAINERS = bool(os.getenv("AUTO_DELETE_COMPLETED_CONTAINERS"))

RESOURCE_GROUP = os.environ["RESOURCE_GROUP"]
DOCKER_TAG = os.environ.get("DOCKER_TAG", "latest")

DELETE_SCHEDULE = os.environ.get("DELETE_SCHEDULE", "*/30 * * * *")
