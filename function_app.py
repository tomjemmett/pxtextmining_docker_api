"""Azure Function for working with Azure Container Instances
"""
import logging
import uuid

import azure.functions as func
from azure.core.exceptions import ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.mgmt.containerinstance import ContainerInstanceManagementClient
from azure.mgmt.containerinstance.models import (
    AzureFileVolume,
    Container,
    ContainerGroup,
    OperatingSystemTypes,
    ResourceRequests,
    ResourceRequirements,
    Volume,
    VolumeMount,
)
from azure.storage.fileshare import ShareDirectoryClient, ShareFileClient

import config

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)


# api routes ----
@app.route(route="StartContainerInstance", methods="POST")
def start_container_instance(req: func.HttpRequest) -> func.HttpResponse:
    """Start running a container from user provided comments

    Users can submit a set of comments as json encoded data. The comments are uploaded to an file
    share, and a container instance is created.

    A random id is generated which is used to name the file in the file share, as well as to name
    the container instance.

    The api will return a 202 response which contains the url to return to in order to collect the
    results if successful.

    :param req: The http request
    :type req: func.HttpRequest
    :return: The http response
    :rtype: func.HttpResponse
    """
    run_id = f"{uuid.uuid4()}"
    logging.info("starting container: %s", run_id)

    comments = req.get_json()
    expected_keys = {"comment_id", "comment_text", "question_type"}
    for i in comments:
        assert set(i.keys()) == expected_keys, "invalid json"

    # don't use the parsed json, we need the binary bytes
    _upload_comments(req.get_body(), run_id)
    _create_and_start_container(run_id, config.DOCKER_TAG)

    results_url = req.url.replace("StartContainerInstance", f"GetResults/{run_id}")
    return func.HttpResponse(results_url, status_code=202)


@app.route(route="GetResults/{run_id:guid}", methods="GET")
def get_results(req: func.HttpRequest) -> func.HttpResponse:
    """Get the results from a model run

    Once the model has run it will save the results to a file in the file share. This api will
    return the contents of this file once it is ready.

    If the results are not ready, it returns a 202 response with the url to collect the results
    from when they are ready.

    If the model runs successfully the api will return a 200 response containing the results. The
    files will be deleted from the file share, and the container group will be deleted.

    If the model fails to run the api will return a 500 response.

    If the model results have previously been collected then the api will return a 404 response.

    :param req: The http request
    :type req: func.HttpRequest
    :return: The http response
    :rtype: func.HttpResponse
    """
    run_id = req.route_params.get("run_id")
    container_id = f"aci-px-{run_id}"

    client = ContainerInstanceManagementClient(
        DefaultAzureCredential(), config.SUBSCRIPTION_ID
    )
    resource_group = config.RESOURCE_GROUP

    try:
        container = (
            client.container_groups.get(resource_group, container_id)
            .containers[0]
            .instance_view.current_state
        )
    except ResourceNotFoundError:
        # check first that the file exists in data_in, if it doesn't then we have
        # likely already collected the file so we should return an error
        if not _check_for_file(run_id):
            return func.HttpResponse("File already collected", status_code=404)
        # otherwise, the container probably hasn't started yet
        return func.HttpResponse(req.url, status_code=202)

    if container.state != "Terminated":
        # if the container is still running then we should ask the user to come
        # back here later
        return func.HttpResponse(req.url, status_code=202)

    if container.detail_status == "Completed":
        client.container_groups.begin_delete(resource_group, container_id)
        return func.HttpResponse(_get_completed_file(run_id), status_code=200)

    return func.HttpResponse("Error during processing", status_code=500)


# helper methods ----
def _upload_comments(comments: bytes, run_id: str) -> None:
    """Uploads the comments file

    :param comments: the comments json as a bytes array
    :type comments: bytes
    :param run_id: the id for the model run
    :type run_id: str
    """
    ShareFileClient(
        config.STORAGE_ENDPOINT,
        "comments",
        f"data_in/{run_id}.json",
        credential=config.STORAGE_KEY,
    ).upload_file(comments)
    logging.info("comments uploaded to storage")


def _create_and_start_container(run_id: str, tag: str = "latest") -> None:
    """Create and start the Azure Container Instance

    :param run_id: the id for the model run
    :type run_id: str
    :param tag: the tag for the docker image to use, defaults to "latest"
    :type tag: str, optional
    """
    client = ContainerInstanceManagementClient(
        DefaultAzureCredential(), config.SUBSCRIPTION_ID
    )

    container_resource_requirements = ResourceRequirements(
        requests=ResourceRequests(
            memory_in_gb=config.CONTAINER_MEMORY, cpu=config.CONTAINER_CPU
        )
    )

    volumes = [
        Volume(
            name="data",
            azure_file=AzureFileVolume(
                share_name="comments",
                storage_account_name=config.STORAGE_ACCOUNT,
                storage_account_key=config.STORAGE_KEY,
                read_only=False,
            ),
        )
    ]

    volume_mounts = [VolumeMount(name="data", mount_path="/data", read_only=False)]

    container = Container(
        name=run_id,
        image=f"{config.CONTAINER_IMAGE}:{tag}",
        resources=container_resource_requirements,
        command=["python3", "docker_run.py", f"{run_id}.json"],
        volume_mounts=volume_mounts,
    )

    cgroup = ContainerGroup(
        location=config.AZURE_LOCATION,
        containers=[container],
        os_type=OperatingSystemTypes.linux,
        restart_policy="Never",
        volumes=volumes,
    )

    client.container_groups.begin_create_or_update(
        config.RESOURCE_GROUP, f"aci-px-{run_id}", cgroup
    )
    logging.info("container created with command: %s", " ".join(container.command))


def _get_completed_file(run_id: str) -> bytes:
    """Get the model run results

    :param run_id: the id for the model run
    :type run_id: str
    :return: the contents of the results
    :rtype: bytes
    """
    client = ShareFileClient(
        config.STORAGE_ENDPOINT,
        "comments",
        f"data_out/{run_id}.json",
        credential=config.STORAGE_KEY,
    )
    file_bytes = client.download_file().readall()
    client.delete_file()
    logging.info("results file downloaded and deleted")
    return file_bytes


def _check_for_file(run_id: str) -> bool:
    """Check if the model run's file exists in the data_in directory

    :param run_id: the id for the model run
    :type run_id: str
    :return: true if the file exists, false if not
    :rtype: bool
    """
    client = ShareDirectoryClient(
        config.STORAGE_ENDPOINT,
        "comments",
        "data_in",
        credential=config.STORAGE_KEY,
    )
    files = [f["name"][:-5] for f in client.list_directories_and_files()]
    return run_id in files
