import os
import logging
from datetime import timezone, datetime

import azure.functions as func
from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.storage.blob import BlobClient, ContainerClient


app = func.FunctionApp()


def run_adf(
    credential,
    subscription_id: str,
    resource_group_name: str,
    data_factory_name: str,
    pipeline_name: str,
    parameters: dict | None = None,
):
    """Launch an Azure Data Factory pipeline.

    @param credential: Azure credential object for authentication
    @param subscription_id: Azure subscription ID
    @param resource_group_name: Name of the resource group
    @param data_factory_name: Name of the data factory
    @param pipeline_name: Name of the pipeline to run
    @param parameters: Optional parameters dict to pass to pipeline (default: empty dict)
    """
    if parameters is None:
        parameters = {}
    adf_client = DataFactoryManagementClient(credential, subscription_id)

    # Kick off the pipeline run
    run_response = adf_client.pipelines.create_run(
        resource_group_name, data_factory_name, pipeline_name, parameters=parameters
    )
    logging.info(
        f"Started ADF pipeline '{pipeline_name}'. RunId: {run_response.run_id}"
    )


def _copy_file(
    credential,
    source_blob_url,
    destination_blob_url,
) -> None:
    """Copy a blob from source to destination using streaming.

    Streams data through this function without requiring temporary files.
    Preserves content type from source to destination. Overwrites existing blob.

    @param credential: Azure credential object for authentication
    @param source_blob_url: Full URL of blob to be copied.
    @param destination_blob_url: Full URL of where blob should be copied.
    """
    _source = BlobClient.from_blob_url(source_blob_url, credential=credential)
    _destination = BlobClient.from_blob_url(destination_blob_url, credential=credential)

    # Stream upload (no temp file). overwrite ensures idempotency on retries.
    stream = _source.download_blob()
    _destination.upload_blob(data=stream.chunks(), overwrite=True)

    try:
        ct = (
            _source.get_blob_properties().content_settings.content_type
            or "application/octet-stream"
        )
        _destination.set_http_headers(content_type=ct)
    except Exception as e:
        logging.error(f"Could not set content type on destination blob: {e}")


def copy_file(
    credential,
    source_storage_account_name: str,
    source_container_name: str,
    source_file_path: str,
    destination_storage_account_name: str,
    destination_container_name: str,
    destination_file_name: str,
) -> None:
    """Copy a blob from source to destination using streaming.

    Streams data through this function without requiring temporary files.
    Preserves content type from source to destination. Overwrites existing blob.

    @param credential: Azure credential object for authentication
    @param source_storage_account_name: Name of source storage account
    @param source_container_name: Name of source container
    @param source_file_path: Path of source blob
    @param destination_storage_account_name: Name of destination storage account
    @param destination_container_name: Name of destination container
    @param destination_file_name: Name/path for destination blob
    """
    source_account_url = f"https://{source_storage_account_name}.blob.core.windows.net"
    destination_account_url = (
        f"https://{destination_storage_account_name}.blob.core.windows.net"
    )

    source_blob_url = f"{source_account_url}/{source_container_name}/{source_file_path}"
    destination_blob_url = (
        f"{destination_account_url}/{destination_container_name}"
        f"/{destination_file_name}"
    )

    _copy_file(
        credential=credential,
        source_blob_url=source_blob_url,
        destination_blob_url=destination_blob_url,
    )


def get_credential() -> ClientSecretCredential:
    """Get credential using App Registration.

    Retrieves tenant ID, client ID, and client secret from environment variables.

    @return: ClientSecretCredential object for authenticating to Azure Storage
    """
    return ClientSecretCredential(
        tenant_id=os.environ["CONTAINER_TENANT_ID"],
        client_id=os.environ["CONTAINER_CLIENT_ID"],
        client_secret=os.environ["CONTAINER_CLIENT_SECRET"],
    )


def parse_event_subject(subject: str) -> tuple[str, str] | tuple[None, None]:
    """Parse Event Grid blob subject to extract container and blob path.

    Splits the Event Grid subject string to extract the container name and blob path.
    Returns (None, None) if the subject format is invalid.

    @param subject: Event Grid subject string in format
        /blobServices/default/containers/<container>/blobs/<path>
    @return: Tuple of (container_name, blob_path) or (None, None) if parsing fails
    """
    try:
        container = subject.split("/containers/")[1].split("/blobs/")[0]
        blob_path = subject.split("/blobs/")[1]
        return container, blob_path
    except Exception as e:
        logging.error(f"Could not parse subject: {e}")
        return None, None


def extract_blob_components(blob_name: str) -> tuple[str, str, str]:
    """Extract source folder, filename, and file extension from a blob path.

    Parses the blob name to identify the source folder (first path segment),
    filename (last path segment), and extension. Falls back to environment
    variable SOURCE_FOLDER if blob path has no directory component.

    @param blob_name: Full blob path (may include directory separators)
    @return: Tuple of (file without suffix, file name with suffix, extension/suffix)
        where extension includes the dot (e.g., ".csv")
    """
    parts = blob_name.split("/")
    file_only = parts[-1]
    file_without_suffix = ""
    try:
        file_without_suffix = file_only[: file_only.index(".")]
    except Exception:
        pass

    ext = "." + (file_only.split(".")[-1].lower() if "." in file_only else "")
    return file_without_suffix, file_only, ext


def process_new_blob(
    azeventgrid: func.EventGridEvent,
    env_var_prefix: str,
    env_var_dest_id: int,
    *,
    expected_destination_filename: str | None = None,
):
    """
    Copies blob into desired locatons. Usually for CSV files
    provided for SF and Qx.

    :param azeventgrid: Event details for extracting info about file
    :type azeventgrid: func.EventGridEvent
    :param env_var_prefix: What prefix for environmental variables is used.
    :type env_var_prefix: str
    :param env_var_dest_id: What is ordinary number of prefixed env variables.
    :type env_var_dest_id: int
    :param expected_destination_filename: Forces the concrete blob name.
    :type expected_destination_filename: str | None
    """
    # Auth for storage: use App Registration
    storage_cred = get_credential()

    # subject: /blobServices/default/containers/<container>/blobs/<path>
    subject = str(azeventgrid.subject)
    logging.info(f"Event subject: {subject}")

    # Extract container and blob path from subject
    container_name, blob_name = parse_event_subject(subject)
    if container_name is None or container_name != os.getenv(
        f"{env_var_prefix}_SOURCE_CONTAINER"
    ):
        logging.info(f"Wrong container!")
        return

    logging.info(f'New file "{blob_name}" in container "{container_name}"')

    if not subject.lower().endswith(".csv"):
        logging.info(f"Not a CSV, skipping!")
        return

    destination_file_name = blob_name
    if expected_destination_filename:
        destination_file_name = expected_destination_filename
    if dest_folder := os.getenv(
        f"{env_var_prefix}_{env_var_dest_id}_DESTINATION_FOLDER", False
    ):
        if dest_folder[-1] != "/":
            dest_folder += "/"
        destination_file_name = dest_folder + destination_file_name

    iso_now: str = datetime.now(timezone.utc).isoformat()
    _dest_file, _, _ext = extract_blob_components(blob_name)
    destination_archive_file_name = f"{_dest_file}-{iso_now}{_ext}"
    if destination_archive_folder := os.getenv(
        f"{env_var_prefix}_{env_var_dest_id}_DESTINATION_ARCHIVE_FOLDER", "archive/"
    ):
        if destination_archive_folder[-1] != "/":
            destination_archive_folder += "/"
        destination_archive_file_name = (
            destination_archive_folder + destination_archive_file_name
        )

    # Copy to destination
    logging.info(f"Copying to the file '{destination_file_name}'.")
    copy_file(
        credential=storage_cred,
        source_storage_account_name=os.getenv(
            f"{env_var_prefix}_SOURCE_STORAGE_ACCOUNT"
        ),
        source_container_name=os.getenv(f"{env_var_prefix}_SOURCE_CONTAINER"),
        source_file_path=blob_name,
        destination_storage_account_name=os.getenv(
            f"{env_var_prefix}_{env_var_dest_id}_DESTINATION_STORAGE_ACCOUNT"
        ),
        destination_container_name=os.getenv(
            f"{env_var_prefix}_{env_var_dest_id}_DESTINATION_CONTAINER"
        ),
        destination_file_name=destination_file_name,
    )

    # Copy to archive
    logging.info(f"Copying to archive file '{destination_archive_file_name}'.")
    copy_file(
        credential=storage_cred,
        source_storage_account_name=os.getenv(
            f"{env_var_prefix}_SOURCE_STORAGE_ACCOUNT"
        ),
        source_container_name=os.getenv(f"{env_var_prefix}_SOURCE_CONTAINER"),
        source_file_path=blob_name,
        destination_storage_account_name=os.getenv(
            f"{env_var_prefix}_{env_var_dest_id}_DESTINATION_STORAGE_ACCOUNT"
        ),
        destination_container_name=os.getenv(
            f"{env_var_prefix}_{env_var_dest_id}_DESTINATION_CONTAINER"
        ),
        destination_file_name=destination_archive_file_name,
    )


def delete_target_url_folder(
    cred,
    target_url: str,
    path_prefix: str,
):
    """
    Delete all content of the folder in the path within storage account.
    
    :param cred: Credentials for access
    :param target_url: Where to delete (URL)
    :param path_prefix: Prefix after URL
    """
    # Extract info:
    _url_target = r".blob.core.windows.net"
    if target_url.endswith("/"):
        target_url = target_url[:-1]
    if path_prefix.endswith("/"):
        path_prefix = path_prefix[:-1]
    account_name = target_url[len(r"https://"):target_url.index(_url_target)]
    _path_suffix_splt = target_url[target_url.index(_url_target) + len(_url_target):].split(r'/')
    container_name = _path_suffix_splt[1]
    if len(_path_suffix_splt) > 2:
        path_prefix = "/".join(_path_suffix_splt[2:]) + "/" + path_prefix
    # Construct path:
    account_url = f"https://{account_name}.blob.core.windows.net"
    container = ContainerClient(account_url, container_name, credential=cred)

    for blob in container.list_blobs(name_starts_with=path_prefix):
        blob_name: str = blob.name
        if blob_name.endswith(path_prefix):
            continue
        try:
            container.delete_blob(
                blob_name, delete_snapshots="include"
            )
        except Exception as e:
            logging.warning(f"error when deleting `{blob_name}`: {str(e)}.")


@app.event_grid_trigger(arg_name="azeventgrid")
def NewBlobCreatedAlderHey(azeventgrid: func.EventGridEvent):
    """
    Process new CSV file arriving from Alder Hey source.

    :param azeventgrid: Event details (like a file name)
    :type azeventgrid: func.EventGridEvent
    """
    env_var_prefix = "ALDER_HEY"
    for env_var_dest_id in range(20):
        if not bool(
            os.getenv(
                f"{env_var_prefix}_{env_var_dest_id}_DESTINATION_STORAGE_ACCOUNT", False
            )
        ):
            continue
        process_new_blob(
            azeventgrid=azeventgrid,
            env_var_prefix=env_var_prefix,
            env_var_dest_id=env_var_dest_id,
        )


@app.event_grid_trigger(arg_name="azeventgrid")
def NewBlobCreatedLCC(azeventgrid: func.EventGridEvent):
    """
    Process new CSV file arriving from LCC source.

    :param azeventgrid: Event details (like a file name)
    :type azeventgrid: func.EventGridEvent
    """
    env_var_prefix = "LCC"
    for env_var_dest_id in range(20):
        if not bool(
            os.getenv(
                f"{env_var_prefix}_{env_var_dest_id}_DESTINATION_STORAGE_ACCOUNT", False
            )
        ):
            continue
        process_new_blob(
            azeventgrid=azeventgrid,
            env_var_prefix=env_var_prefix,
            env_var_dest_id=env_var_dest_id,
            # expected_destination_filename=None
            expected_destination_filename="CYPRiskFactors_LCC.csv",
        )


@app.event_grid_trigger(arg_name="azeventgrid")
def ZippedBakArrivedForSocialFinance(azeventgrid: func.EventGridEvent):
    """
    This Azure Function triggers an ADF pipeline which
    unzips serialized MS-SQL database bak file,
    loads it into a MS-SQL Managed Instance resource
    and distribute it where required (uses HeifER project).

    :param azeventgrid: Blob event itself.
    """
    # subject: /blobServices/default/containers/<container>/blobs/<path>
    subject = str(azeventgrid.subject)
    logging.info(f"Event subject: {subject}")

    # Extract container and blob path from subject
    container_name, blob_name = parse_event_subject(subject)
    if container_name is None or container_name != os.getenv(f"SF_BAK_CONTAINER"):
        logging.info(f"Wrong container!")
        return

    logging.info(f'New file "{blob_name}" in container "{container_name}"')

    if not subject.lower().endswith(".zip"):
        logging.info(f"Not a ZIP, skipping!")
        return

    run_adf(
        credential=get_credential(),
        subscription_id=os.getenv(f"SF_BAK_ADF_SUBSCRIPTION", None),
        resource_group_name=os.getenv(f"SF_BAK_ADF_RESOURCE_GROUP", None),
        data_factory_name=os.getenv(f"SF_BAK_ADF_NAME", None),
        pipeline_name=os.getenv(f"SF_BAK_ADF_PIPELINE_NAME", None),
    )


@app.event_grid_trigger(arg_name="azeventgrid")
def CopyParquetFileFromQuantexaToSocialFinance(azeventgrid: func.EventGridEvent):
    """
    This method copies Parquet folders (aka file) from given location into
    destination locations defined by the set of URLs (located in Storage Accounts).

    :param azeventgrid: Definition of the event.
    """
    # subject: /blobServices/default/containers/<container>/blobs/<path>
    subject = str(azeventgrid.subject)
    logging.info(f"Event subject: {subject}")

    # Extract container and blob path from subject
    container_name, blob_name = parse_event_subject(subject)
    if container_name is None or container_name != (
        _exp_cont := (os.getenv(r"QX_PARQUET_FOR_SF_SOURCE_CONTAINER"))
    ):
        logging.info(f'Wrong container "{container_name}", but expected "{_exp_cont}"!')
        return

    logging.info(f'New file "{blob_name}" in container "{container_name}"')
    if not subject.lower().endswith(r".parquet/_success"):
        logging.info(r"Not an Apache Parquet file!")
        return
    # Extract the name of the folder (i.e. of this concrete Parquet file)
    blob_folder = blob_name[: -len("/_success")]
    # Folder where all Parquet files are located (one above the previous var)
    parquet_file_folder: str = os.getenv(r"QX_PARQUET_FOR_SF_SOURCE_FOLDER_PATH")
    if parquet_file_folder[-1] != "/":
        # Make sure it's a folder
        parquet_file_folder += "/"
    # We want to be absolutely sure that our blob is in this folder
    if not blob_name.startswith(parquet_file_folder):
        logging.info(r"Wrong location of Apache Parquet file!")
        return
    parquet_folder = blob_folder[len(parquet_file_folder):]
    
    logging.info(r"Processing file.")

    # Source storage account
    storage_account = os.getenv(r"QX_PARQUET_FOR_SF_SOURCE_STORAGE_ACCOUNT")
    account_url = f"https://{storage_account}.blob.core.windows.net"
    client_container = ContainerClient(
        account_url, container_name, credential=get_credential()
    )

    # Essentially a folder for creating archive structure:
    archive_folder_prefix = os.getenv(
        r"QX_PARQUET_FOR_SF_DESTINATION_ARCHIVE", "archive"
    )
    # Essentially a folder for creating latest files structure:
    latest_folder_prefix = os.getenv(r"QX_PARQUET_FOR_SF_DESTINATION_LATEST", "latest")
    if latest_folder_prefix.endswith('/'):
        latest_folder_prefix = latest_folder_prefix[:-1]
    # Vertical bar-separated list of URLs
    #   With logic: https://<storage-account>.blob.core.windows.net/<container>/<path>
    destination_paths = os.getenv(r"QX_PARQUET_FOR_SF_DESTINATION_URLS").split("|")

    # List all actual files available in the Parquet file (which is a folder)
    all_blobs_in_folder = [
        blob.name
        for blob in client_container.list_blobs(name_starts_with=parquet_file_folder)
    ]

    # Get current date and time to create archive logic
    iso_now: str = datetime.now(timezone.utc).isoformat()
    source_account_url = f"https://{storage_account}.blob.core.windows.net"
    credential = get_credential()
    
    # Clean target folders
    for destination_path in destination_paths:
        if destination_path.endswith("/"):
            destination_path = destination_path[:-1]
        delete_target_url_folder(credential,
                                 destination_path, 
                                 latest_folder_prefix + "/" + parquet_folder)
    
    for _blob_copied in all_blobs_in_folder:
        if not str(_blob_copied).startswith(blob_folder):
            # Only blobs in a current Parquet file (which happened to be a folder)
            continue
        logging.info(f"Processing blob {str(_blob_copied)}.")
        # The following is added as folder itself is listed but cannot be directly
        #   copied, hence the only way is to skip it.
        if str(_blob_copied).lower().endswith(r".parquet"):
            # To skip folders (nuisance caused by the fact that everything is listed):
            skip_loop = False
            for _other_blobs in all_blobs_in_folder:
                if str(_other_blobs).startswith(_blob_copied) and str(
                    _other_blobs
                ).startswith(_blob_copied + "/"):
                    skip_loop = True
                    continue
            if skip_loop:
                continue
        # Clean folders
        # Performs the copying to every desired location:
        for destination_path in destination_paths:
            if destination_path.endswith("/"):
                destination_path = destination_path[:-1]
            # Define source and destination URLs
            source_blob_url = f"{source_account_url}/{container_name}/{_blob_copied}"
            destination_blob = _blob_copied[len(parquet_file_folder) :]
            destination_blob_url_latest = (
                f"{destination_path}/{latest_folder_prefix}/{destination_blob}"
            )
            # Copy to the latest folder
            _copy_file(
                credential=credential,
                source_blob_url=source_blob_url,
                destination_blob_url=destination_blob_url_latest,
            )
            # Copy to archive
            destination_blob_url_archive = (
                f"{destination_path}/{archive_folder_prefix}/{iso_now}/{destination_blob}"
            )
            _copy_file(
                credential=credential,
                source_blob_url=source_blob_url,
                destination_blob_url=destination_blob_url_archive,
            )
