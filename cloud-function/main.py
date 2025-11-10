import json
import os
import re
import time
from typing import Optional, Dict, Any
from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import NotFound, GoogleCloudError, Forbidden
from google.api_core import exceptions as api_exceptions
from google.auth import default
from google.auth.exceptions import GoogleAuthError, RefreshError
import logging
from functools import wraps
import functions_framework
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
import socket
import requests
from google.cloud import secretmanager


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration from environment
_secret_client = None
_secrets_loaded = False
SENDGRID_API_KEY = None
FROM_EMAIL = None
EMAIL_ENABLED: bool = os.environ.get('EMAIL_ENABLED', 'true').lower() == 'true'

MAX_RETRIES = int(os.environ.get('MAX_RETRIES', '1'))
RETRY_DELAY = int(os.environ.get('RETRY_DELAY', '5'))  # seconds
MAX_FILE_SIZE_MB = int(os.environ.get('MAX_FILE_SIZE_MB', '1000'))
TIMEOUT_SECONDS = int(os.environ.get('TIMEOUT_SECONDS', '540'))


def _get_secret(project_id: str, secret_id: str) -> str:
    """Get secret from Secret Manager. Raises SecretError if secret not found or access denied."""
    global _secret_client
    try:
        if _secret_client is None:
            _secret_client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        secret_value = _secret_client.access_secret_version(request={"name": name}).payload.data.decode("UTF-8")
        if not secret_value or not secret_value.strip():
            raise SecretError(f"Secret '{secret_id}' is empty or invalid in project {project_id}")
        return secret_value
    except NotFound:
        raise SecretError(f"Secret '{secret_id}' not found in project {project_id}. Please create the secret in Secret Manager.")
    except Forbidden:
        raise SecretError(f"Permission denied accessing secret '{secret_id}' in project {project_id}. Ensure the service account has 'roles/secretmanager.secretAccessor' role.")
    except SecretError:
        raise  # Re-raise SecretError as-is
    except Exception as e:
        raise SecretError(f"Error retrieving secret '{secret_id}' from Secret Manager: {str(e)}")



def _load_secrets(project_id: Optional[str] = None) -> None:
    """Load secrets from Secret Manager only. Raises SecretError if secrets are invalid or not found."""
    global SENDGRID_API_KEY, FROM_EMAIL, _secrets_loaded
    if _secrets_loaded:
        return
    
    if not project_id:
        try:
            _, project_id = default()
        except Exception as e:
            return 'OK'
    
    if not project_id:
        raise SecretError("Project ID is required to access secrets from Secret Manager")
    
    # Get secrets from Secret Manager only
    # uncomment
    # SENDGRID_API_KEY = _get_secret(project_id, 'POS_LOAD_SENDGRID_KEY')
    # FROM_EMAIL = _get_secret(project_id, 'POS_LOAD_SENDGRID_FROM')

    SENDGRID_API_KEY = os.environ.get('SENDGRID_API_KEY')
    FROM_EMAIL = os.environ.get('FROM_EMAIL')

    print(FROM_EMAIL,  '    ', SENDGRID_API_KEY)
    
    # Validate secrets are not empty (additional check)
    if not SENDGRID_API_KEY or not SENDGRID_API_KEY.strip():
        raise SecretError("SENDGRID_API_KEY is empty or invalid")
    if not FROM_EMAIL or not FROM_EMAIL.strip():
        raise SecretError("FROM_EMAIL is empty or invalid")
    
    _secrets_loaded = True



class ConfigValidationError(Exception):
    """Raised when config file validation fails"""
    pass


class DataLoadError(Exception):
    """Raised when BigQuery data load fails"""
    pass


class FileProcessingError(Exception):
    """Raised when file processing fails"""
    pass


class PermissionError(Exception):
    """Raised when permission errors occur"""
    pass


class QuotaExceededError(Exception):
    """Raised when quota limits are exceeded"""
    pass


class MemoryLimitError(Exception):
    """Raised when memory limits are exceeded"""
    pass


class InvalidCSVFormatError(Exception):
    """Raised when CSV format is invalid"""
    pass


class TableConflictError(Exception):
    """Raised when table operation conflicts occur"""
    pass

class SecretError(Exception):
    """Raised when secret retrieval or validation fails"""
    pass



def retry_on_failure(max_retries: int = MAX_RETRIES, delay: int = RETRY_DELAY):
    """Decorator to retry function calls on failure"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except (GoogleCloudError, ConnectionError, TimeoutError, api_exceptions.InternalServerError, api_exceptions.ServiceUnavailable) as e:
                    if is_quota_error(e):
                        wait_time = delay * (2 ** attempt) * 2  
                        logger.warning(
                            f"Quota error on attempt {attempt + 1} for {func.__name__}: {str(e)}. "
                            f"Retrying in {wait_time} seconds..."
                        )
                    elif is_network_error(e):
                        wait_time = delay * (2 ** attempt)  
                        logger.warning(
                            f"Network error on attempt {attempt + 1} for {func.__name__}: {str(e)}. "
                            f"Retrying in {wait_time} seconds..."
                        )
                    else:
                        wait_time = delay * (2 ** attempt)
                        logger.warning(
                            f"Attempt {attempt + 1} failed for {func.__name__}: {str(e)}. "
                            f"Retrying in {wait_time} seconds..."
                        )
                    
                    last_exception = e
                    if attempt < max_retries - 1:
                        time.sleep(wait_time)
                    else:
                        logger.error(f"All {max_retries} attempts failed for {func.__name__}")
                        if is_quota_error(e):
                            logger.error(f"Quota exceeded after {max_retries} attempts: {str(e)}")
                            raise QuotaExceededError(f"All retries failed for {func.__name__}: {str(e)}")
                except (PermissionError, ConfigValidationError, FileNotFoundError, DataLoadError, 
                        TableConflictError, InvalidCSVFormatError, MemoryLimitError, Forbidden, NotFound, SecretError) as e:
                    logger.error(f"Non-retryable error in {func.__name__}: {str(e)}")
                    raise e 
                except Exception as e:
                    logger.error(f"Unexpected error in {func.__name__}: {str(e)}")
                    raise e 
            if last_exception:
                logger.error(f"All retries exhausted for {func.__name__}: {str(last_exception)}")
                raise last_exception
            raise DataLoadError(f"{func.__name__} failed after all retries without a specific exception.")
        return wrapper
    return decorator


def validate_email(email: str) -> bool:
    """Validate email address format"""
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None


def validate_table_name(table_name: str) -> str:
    """Validate and sanitize BigQuery table name"""
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', table_name)
    if not sanitized or len(sanitized) > 1024:
        raise ConfigValidationError(f"Invalid or excessively long table name: {table_name}")
    if sanitized[0].isdigit():
        sanitized = f"table_{sanitized}"
    return sanitized


def validate_dataset_name(dataset_id: str) -> bool:
    """Validate BigQuery dataset name format"""
    pattern = r'^[a-zA-Z0-9_]{1,1024}$'
    return re.match(pattern, dataset_id) is not None


def validate_config(config: Dict[str, Any]) -> bool:
    """Validate config file structure and values"""
    # required_fields = ['file_location', 'dataset', 'email']
    # missing_fields = [field for field in required_fields if not config.get(field)]
    
    # if missing_fields:
    #     raise ConfigValidationError(f"Missing required fields in config: {', '.join(missing_fields)}")
    
    email = config.get('email')
    if email and not validate_email(email):
        raise ConfigValidationError(f"Invalid email format: {email}")
    
    dataset_id = config.get('dataset')
    if not validate_dataset_name(dataset_id):
        raise ConfigValidationError(f"Invalid dataset name format: {dataset_id}")
    
    file_location = config.get('file_location')
    if not file_location or not isinstance(file_location, str):
        raise ConfigValidationError("file_location must be a non-empty string")
    
    if not validate_gcs_uri(file_location):
        raise ConfigValidationError(f"Invalid GCS URI format for file_location: {file_location}")
    
    override = config.get('override', True)
    if not isinstance(override, bool):
        raise ConfigValidationError("override must be a boolean")
    
    is_header = config.get('is_header', True)
    if not isinstance(is_header, bool):
        raise ConfigValidationError("is_header must be a boolean")
    
    return True


def check_dataset_exists(bq_client: bigquery.Client, dataset_id: str, project_id: str) -> bool:
    """Check if BigQuery dataset exists"""
    try:
        dataset_ref = bq_client.dataset(dataset_id, project=project_id)
        bq_client.get_dataset(dataset_ref)
        return True
    except NotFound:
        raise ConfigValidationError(f"Dataset {dataset_id} does not exist in project {project_id}")
    except Forbidden:
        raise PermissionError(f"Permission denied checking dataset {dataset_id}")
    except Exception as e:
        logger.error(f"Error checking dataset existence: {str(e)}")
        raise FileProcessingError(f"Error checking dataset: {str(e)}")


def check_file_size(blob: storage.Blob, max_size_mb: int = MAX_FILE_SIZE_MB) -> bool:
    """Validate file size is within limits"""
    try:
        blob.reload()
        size_mb = blob.size / (1024 * 1024)
        if size_mb > max_size_mb:
            raise FileProcessingError(f"File size {size_mb:.2f} MB exceeds maximum allowed size {max_size_mb} MB")
        if blob.size == 0:
            raise FileProcessingError("File is empty")
        return True
    except Exception as e:
        logger.error(f"Error checking file size: {str(e)}")
        raise FileProcessingError(f"Error checking file size: {str(e)}")


def check_bucket_exists(storage_client: storage.Client, bucket_name: str) -> bool:
    """Check if GCS bucket exists and is accessible"""
    try:
        bucket = storage_client.bucket(bucket_name)
        return True
    except NotFound:
        raise ConfigValidationError(f"Bucket not found: {bucket_name}")
    except Forbidden:
        raise PermissionError(f"Permission denied accessing bucket: {bucket_name}")
    except Exception as e:
        logger.error(f"Error accessing bucket {bucket_name}: {str(e)}")
        raise FileProcessingError(f"Error accessing bucket {bucket_name}: {str(e)}")


def check_file_permissions(storage_client: storage.Client, bucket_name: str, file_path: str) -> bool:
    """Check if file is accessible and has read permissions"""
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_path)
        if not blob.exists():
            raise FileNotFoundError(f"File not found: gs://{bucket_name}/{file_path}")
        blob.reload()
        return True
    except Forbidden:
        raise PermissionError(f"Permission denied accessing file: gs://{bucket_name}/{file_path}")
    except NotFound:
        raise FileNotFoundError(f"File not found: gs://{bucket_name}/{file_path}")
    except Exception as e:
        logger.error(f"Error checking file permissions: {str(e)}")
        raise FileProcessingError(f"Error checking file permissions: {str(e)}")


def check_file_already_processed(storage_client: storage.Client, bucket_name: str, file_path: str) -> bool:
    """Check if file has already been processed (exists in processed folder)"""
    try:
        processed_path = f"processed/{os.path.basename(file_path)}"
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(processed_path)
        return blob.exists()
    except Exception as e:
        logger.warning(f"Error checking if file already processed: {str(e)}")
        return False



def check_bigquery_permissions(bq_client: bigquery.Client, dataset_id: str, project_id: str) -> bool:
    """Check if we have necessary BigQuery permissions"""
    try:
        dataset_ref = bq_client.dataset(dataset_id, project=project_id)
        bq_client.get_dataset(dataset_ref)
        list(bq_client.list_tables(dataset_ref, max_results=1))
        return True
    except Forbidden:
        raise PermissionError(f"Insufficient permissions for dataset {dataset_id} in project {project_id}")
    except NotFound:
        raise ConfigValidationError(f"Dataset {dataset_id} not found in project {project_id}")
    except Exception as e:
        logger.error(f"Error checking BigQuery permissions: {str(e)}")
        raise FileProcessingError(f"Error checking BigQuery permissions: {str(e)}")


def is_quota_error(error: Exception) -> bool:
    """Check if error is a quota exceeded error"""
    error_str = str(error).lower()
    quota_indicators = [
        'quota',
        'rate limit',
        'too many requests',
        '429',
        'resource exhausted',
        'backend error',
        'concurrent request limit'
    ]
    return any(indicator in error_str for indicator in quota_indicators)


def is_schema_mismatch_error(error: Exception) -> bool:
    """Check if error is a schema mismatch error"""
    error_str = str(error).lower()
    schema_indicators = [
        'schema',
        'field',
        'column',
        'type mismatch',
        'invalid value',
        'cannot convert',
        'expected'
    ]
    return any(indicator in error_str for indicator in schema_indicators)


def is_network_error(error: Exception) -> bool:
    """Check if error is a network-related error"""
    return isinstance(error, (
        ConnectionError,
        TimeoutError,
        socket.timeout,
        socket.error,
        requests.exceptions.RequestException,
        requests.exceptions.ConnectionError,
        requests.exceptions.Timeout,
        api_exceptions.InternalServerError, # 500
        api_exceptions.ServiceUnavailable, # 503
    ))


def validate_gcs_uri(uri: str) -> bool:
    """Validate GCS URI format"""
    if not uri:
        logger.error("GCS URI cannot be empty")
        return False
    
    if uri.startswith('gs://'):
        uri = uri[5:]
    
    parts = uri.split('/', 1)
    if not parts or not parts[0]:
        logger.error(f"Invalid GCS URI format: missing bucket name in {uri}")
        return False
    
    bucket_name = parts[0]
    if not re.match(r'^[a-z0-9._-]+$', bucket_name.lower()):
        logger.error(f"Invalid bucket name format: {bucket_name}")
        return False
    
    return True


def validate_dataset_location(bq_client: bigquery.Client, dataset_id: str, project_id: str, expected_location: str = None) -> bool:
    """Validate dataset location matches expected location"""
    try:
        dataset_ref = bq_client.dataset(dataset_id, project=project_id)
        dataset = bq_client.get_dataset(dataset_ref)
        
        if expected_location and dataset.location.lower() != expected_location.lower():
            logger.warning(
                f"Dataset location mismatch: dataset is in {dataset.location}, "
                f"expected {expected_location}"
            )
        return True
    except NotFound:
        raise ConfigValidationError(f"Dataset {dataset_id} not found in project {project_id}")
    except Exception as e:
        logger.warning(f"Error validating dataset location: {str(e)}")
        return False


def validate_csv_basic_format(blob: storage.Blob) -> bool:
    """Perform basic CSV format validation"""
    try:
        sample_size = min(1024, blob.size)
        if sample_size == 0:
            raise InvalidCSVFormatError("CSV file is empty")
        
        sample = blob.download_as_bytes(start=0, end=sample_size)
        sample_text = sample.decode('utf-8', errors='ignore')
        
        if not sample_text.strip():
            raise InvalidCSVFormatError("CSV file appears to be empty or whitespace only")
        
        has_comma = ',' in sample_text
        has_tab = '\t' in sample_text
        has_pipe = '|' in sample_text
        
        if not (has_comma or has_tab or has_pipe):
            logger.warning("CSV file may not have standard delimiters (comma, tab, pipe)")
        
        if '\n' not in sample_text and '\r' not in sample_text:
            logger.warning("CSV file may not have row separators")
        
        return True
        
    except UnicodeDecodeError as e:
        raise InvalidCSVFormatError(f"CSV file encoding error: {str(e)}")
    except Exception as e:
        logger.warning(f"Error validating CSV format: {str(e)}")
        return False


@retry_on_failure()
def load_data_to_bigquery(
    bq_client: bigquery.Client,
    gcs_uri: str,
    dataset_id: str,
    table_name: str,
    project_id: str,
    is_header: bool,
    override: bool,
    timeout: int = TIMEOUT_SECONDS
) -> bigquery.LoadJob:
    """Load data from GCS to BigQuery with retry logic"""
    try:
        check_dataset_exists(bq_client, dataset_id, project_id)
        check_bigquery_permissions(bq_client, dataset_id, project_id)
        
        table_ref = bq_client.dataset(dataset_id, project=project_id).table(table_name)

        if override:
            try:
                bq_client.delete_table(table_ref, not_found_ok=True)
                logger.info(f"Deleted existing table {dataset_id}.{table_name} for replacement (override=True)")
            except Exception as e:
                logger.warning(f"Could not delete table {dataset_id}.{table_name} before replacement: {str(e)}. Will use WRITE_TRUNCATE instead.")

        skip_leading_rows = 1 if is_header else 0
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=skip_leading_rows,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE if override else bigquery.WriteDisposition.WRITE_APPEND,
            max_bad_records=10,
            ignore_unknown_values=False,
            allow_quoted_newlines=True,
            allow_jagged_rows=False,
        )
        
        logger.info(f"Starting BigQuery load job for {gcs_uri} -> {dataset_id}.{table_name}")
        
        load_job = bq_client.load_table_from_uri(
            gcs_uri,
            table_ref,
            job_config=job_config
        )
        
        logger.info(f"BigQuery job ID: {load_job.job_id}")
        
        try:
            load_job.result(timeout=timeout)
        except (TimeoutError, Exception) as e:
            try:
                bq_client.cancel_job(load_job.job_id)
                logger.warning(f"Cancelled BigQuery job {load_job.job_id} due to timeout or error: {str(e)}")
            except Exception as cancel_e:
                logger.error(f"Failed to cancel job {load_job.job_id}: {str(cancel_e)}")
            raise DataLoadError(f"BigQuery load job exceeded timeout of {timeout} seconds or failed: {str(e)}")
        
        load_job.reload()
        
        if load_job.state != 'DONE':
            raise DataLoadError(f"BigQuery job {load_job.job_id} did not complete. State: {load_job.state}")
        
        if load_job.error_result:
            error_message = load_job.error_result.get('message', 'Unknown error')
            error_details = json.dumps(load_job.error_result, indent=2)
            logger.error(f"BigQuery job failed: {error_message}")
            logger.error(f"Error details: {error_details}")
            
            if load_job.errors:
                logger.error(f"Job errors: {json.dumps(load_job.errors, indent=2)}")
            
            if is_quota_error(Exception(error_message)):
                raise QuotaExceededError(f"BigQuery quota exceeded: {error_message}")
            elif is_schema_mismatch_error(Exception(error_message)):
                raise InvalidCSVFormatError(
                    f"Schema mismatch in BigQuery load job {load_job.job_id}: {error_message}. "
                    f"Please check CSV format and column types."
                )
            
            raise DataLoadError(f"BigQuery load job {load_job.job_id} failed: {error_message}")
        
        bad_records = 0
        if hasattr(load_job, 'output_rows') and load_job.output_rows is not None:
            if hasattr(load_job, 'statistics') and hasattr(load_job.statistics, 'load') and hasattr(load_job.statistics.load, 'bad_records'):
                bad_records = load_job.statistics.load.bad_records
        
        logger.info(
            f"Load completed. Rows loaded: {load_job.output_rows}, "
            f"Bad records: {bad_records}"
        )
        
        if bad_records > 0:
            logger.warning(f"Warning: {bad_records} bad records were skipped during load")
        
        return load_job
        
    except (NotFound, Forbidden, GoogleCloudError, api_exceptions.ResourceExhausted) as e:
        logger.error(f"BigQuery operation failed: {str(e)}")
        if is_quota_error(e):
            raise QuotaExceededError(f"BigQuery quota exceeded: {str(e)}")
        elif isinstance(e, Forbidden):
            raise PermissionError(f"Permission denied for BigQuery operation: {str(e)}")
        elif isinstance(e, NotFound):
            raise ConfigValidationError(f"BigQuery resource not found: {str(e)}")
        else:
            raise DataLoadError(f"Google Cloud error during BigQuery load: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error during BigQuery load: {str(e)}")
        raise DataLoadError(f"Unexpected error during BigQuery load: {str(e)}")


def move_file_safely(
    storage_client: storage.Client,
    bucket_name: str,
    source_path: str,
    dest_path: str,
    operation_name: str
) -> None:
    """Safely move file with error handling"""
    try:
        bucket = storage_client.bucket(bucket_name)
        source_blob = bucket.blob(source_path)
        
        if not source_blob.exists():
            logger.warning(f"Source file does not exist: {source_path}, skipping move")
            return
        
        # Copy to destination
        try:
            dest_blob = bucket.copy_blob(source_blob, bucket, dest_path)
        except Forbidden:
            raise PermissionError(f"Permission denied copying file to {dest_path}")
        except (ConnectionError, TimeoutError, GoogleCloudError) as e:
            raise FileProcessingError(f"Network error copying file: {str(e)}")
        
        if not dest_blob.exists():
            raise FileProcessingError(f"Failed to copy file to {dest_path} - destination not found")
        
        # Delete source file
        try:
            source_blob.delete()
        except Forbidden:
            logger.warning(f"Permission denied deleting source file: {source_path}")
        except (ConnectionError, TimeoutError, GoogleCloudError) as e:
            logger.warning(f"Network error deleting source file: {str(e)}")
        except Exception as e:
            logger.warning(f"Error deleting source file: {str(e)}")
        
        logger.info(f"Successfully moved file for {operation_name}: {source_path} -> {dest_path}")
        
    except Exception as e:
        logger.error(f"Error moving file {source_path} to {dest_path}: {str(e)}")
        raise FileProcessingError(f"Failed to move file: {str(e)}")


@retry_on_failure(max_retries=1, delay=5)
def send_email_notifications(to_email: str, subject: str, content: str, is_error: bool = False, project_id: Optional[str] = None) -> None:
    """
    Send email notification about the Cloud Function execution status.
    """
    if not EMAIL_ENABLED:
        logger.info("Skipping email send (EMAIL_ENABLED is False).")
        return
    
    if not to_email:
        logger.warning("Skipping email send (no recipient email provided).")
        return

    try:
        _load_secrets(project_id)
    except SecretError as e:
        logger.error(f"Secret validation failed: {str(e)}")
        raise SecretError("Secret validation failed from Secret Manager")
    
    message = Mail(
        from_email=FROM_EMAIL,
        to_emails=to_email,
        subject=subject,
        html_content=content
    )

    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)
        logger.info(f"Email sent via SendGrid. Status: {response.status_code}")

    except Exception as e:
        logger.error(f"Error sending email notification: {str(e)}")


@functions_framework.cloud_event
def process_config_file(cloud_event):
    """
    Cloud Function that triggers when a config.json file is uploaded to GCS bucket.
    Reads the config file to get file location and loads data into BigQuery.
    """
    config_file_name = None
    bucket_name = None
    email = None
    full_table_id = "N/A"
    gcs_uri = "N/A"
    project_id = None
    storage_client = None
    bq_client = None
    
    try:
        if not cloud_event or not hasattr(cloud_event, 'data'):
            raise ConfigValidationError("Invalid cloud event: missing data")
        
        data = cloud_event.data
        config_file_name = data.get('name')
        bucket_name = data.get('bucket')
        
        if not config_file_name:
            raise ConfigValidationError("Cloud event missing 'name' field")
        if not bucket_name:
            raise ConfigValidationError("Cloud event missing 'bucket' field")
        
        logger.info(f"Processing config file: {config_file_name} from bucket: {bucket_name}")
        
        if not config_file_name.lower().endswith('_config.json'):
            logger.warning(f"Skipping non-config file: {config_file_name}")
            return 'OK' 
        
        if "processed/" in config_file_name.lower():
            logger.warning(f"Skipping processed file: {config_file_name}")
            return 'OK'
        
        try:
            credentials, project_id = default()
            if hasattr(credentials, 'expired') and credentials.expired:
                try:
                    credentials.refresh(None)
                except RefreshError as e:
                    raise GoogleAuthError(f"Credentials expired and refresh failed: {str(e)}")
        except GoogleAuthError as e:
            raise GoogleAuthError(f"Failed to authenticate with GCP: {str(e)}")
        except RefreshError as e:
            raise GoogleAuthError(f"Failed to refresh expired credentials: {str(e)}")
        
        try:
            storage_client = storage.Client(project=project_id)
            bq_client = bigquery.Client(project=project_id)
        except Exception as e:
            raise ConnectionError(f"Failed to initialize GCP clients: {str(e)}")
        
        try:
            check_bucket_exists(storage_client, bucket_name)
            check_file_permissions(storage_client, bucket_name, config_file_name)
            
            bucket = storage_client.bucket(bucket_name)
            config_blob = bucket.blob(config_file_name)
            
            if not config_blob.exists():
                raise FileNotFoundError(f"Config file not found: {config_file_name}")
            
            check_file_size(config_blob, max_size_mb=10)
            
            try:
                config_content = config_blob.download_as_text()
            except (Forbidden, ConnectionError, TimeoutError, MemoryError, OSError) as e:
                raise FileProcessingError(f"Failed to read config file: {str(e)}")
            except Exception as e:
                raise FileProcessingError(f"Failed to read config file: {str(e)}")
            
            try:
                config = json.loads(config_content)
            except json.JSONDecodeError as e:
                raise ConfigValidationError(f"Invalid JSON in config file: {str(e)}")
            
            logger.info(f"Config loaded: {json.dumps(config, indent=2)}")
            
        except Exception as e:
            logger.error(f"Error accessing config file: {str(e)}")
            raise
        
        data_file_name = config_file_name.split('/')[-1].removesuffix("_config.json")
        
        email = config.get('email')
        if email and not validate_email(email):
            raise ConfigValidationError(f"Invalid email format: {email}")
    
        dataset_id = config.get('dataset')
        if not dataset_id or not validate_dataset_name(dataset_id):
            raise ConfigValidationError(f"Invalid dataset name format: {dataset_id}")
    
        file_location = config.get('file_location')
        if not file_location or not isinstance(file_location, str):
            raise ConfigValidationError("file_location must be a non-empty string")

        override = config.get('override', True)
        tablename = config.get('tablename') or data_file_name
        is_header = config.get('is_header', True)

        # Validate config structure
        validate_config(config)
        
        if not data_file_name:
            raise ConfigValidationError("Could not extract data file name from config file name")
        
        table_name = validate_table_name(tablename.replace('-', '_').replace(' ', '_'))
        full_table_id = f"{dataset_id}.{table_name}"
        
        logger.info(f"File location: {file_location}")
        logger.info(f"Email: {email}")
        logger.info(f"Dataset: {dataset_id}")
        logger.info(f"Table name: {table_name}")
        logger.info(f"Full table ID: {full_table_id}")
        logger.info(f"Override: {override}")
        logger.info(f"Is header: {is_header}")
        
        
        if file_location.startswith('gs://'):
            file_location = file_location[5:]
        
        parts = file_location.split('/', 1)
        if not parts or not parts[0]:
            raise ConfigValidationError("Invalid file_location format: missing bucket name")
        
        data_bucket_name = parts[0]
        data_file_path = parts[1] if len(parts) > 1 else ""
        
        logger.info(f"Data bucket: {data_bucket_name}")
        logger.info(f"Data file path: {data_file_path}")
        logger.info(f"Data file name: {data_file_name}.csv")
        
        if data_file_path and not data_file_path.endswith('/'):
            data_file_path += '/'
        
        try:
            check_bucket_exists(storage_client, data_bucket_name)
            
            data_file_full_path = f"{data_file_path}{data_file_name}.csv"
            
            check_file_permissions(storage_client, data_bucket_name, data_file_full_path)
            
            data_bucket = storage_client.bucket(data_bucket_name)
            data_blob = data_bucket.blob(data_file_full_path)
            
            if not data_blob.exists():
                raise FileNotFoundError(f"Data file not found: {data_file_full_path} in bucket {data_bucket_name}")
            
            logger.info(f"Data file exists: gs://{data_bucket_name}/{data_file_full_path}")
            
            check_file_size(data_blob, max_size_mb=MAX_FILE_SIZE_MB)
            
            try:
                validate_csv_basic_format(data_blob)
            except InvalidCSVFormatError as e:
                raise e
            except Exception as e:
                logger.warning(f"CSV format validation warning: {str(e)}")
            
            logger.info(f"Data file validated: {data_file_name}.csv ({data_blob.size / (1024*1024):.2f} MB)")
            
        except Exception as e:
            logger.error(f"Error accessing data file: {str(e)}")
            raise 
        
        gcs_uri = f'gs://{data_bucket_name}/{data_file_path}{data_file_name}.csv'
        logger.info(f"Loading data from: {gcs_uri}")
        
        try:
            validate_dataset_location(bq_client, dataset_id, project_id)
        except Exception as e:
            logger.warning(f"Dataset location validation warning: {str(e)}")
        

        load_job = load_data_to_bigquery(
            bq_client=bq_client,
            gcs_uri=gcs_uri,
            dataset_id=dataset_id,
            table_name=table_name,
            project_id=project_id,
            is_header=is_header,
            override=override,
            timeout=TIMEOUT_SECONDS
        )
        
        logger.info(f"BigQuery load from {gcs_uri} to {full_table_id} completed successfully.")
        logger.info(f"BigQuery job state: {load_job.state}")
        
        try:
            processed_data_path = f"processed/{data_file_name}.csv"
            logger.info(f"Moving data file to processed folder: {processed_data_path}")
            move_file_safely(
                storage_client,
                data_bucket_name,
                f"{data_file_path}{data_file_name}.csv",
                processed_data_path,
                "data file processing"
            )
            logger.info(f"Data file moved to: gs://{data_bucket_name}/{processed_data_path}")
            
            config_file = os.path.basename(config_file_name)
            processed_config_path = f"processed/{config_file}"
            logger.info(f"Moving config file to processed folder: {processed_config_path}")
            move_file_safely(
                storage_client,
                bucket_name,
                config_file_name,
                processed_config_path,
                "config file processing"
            )
            logger.info(f"Config file moved to: gs://{bucket_name}/{processed_config_path}")
            
        except (FileProcessingError, PermissionError) as e:
            logger.error(f"Failed to move files to processed folder: {str(e)}. "
                         "Data load was successful, but files require manual cleanup.")

            subject = f"Warning: BQ Load Success, Cleanup Failed - {full_table_id}"
            body = f"""<html>
                <body>
                <h2>BigQuery Data Load Completed, but File Cleanup Failed</h2>
                <p><strong>Table:</strong> {full_table_id}</p>
                <p><strong>CSV File Path:</strong> {gcs_uri}</p>
                <p><strong>Config File:</strong> {config_file_name}</p>
                <p><strong>Error:</strong> Failed to move files to 'processed' folder: {str(e)}</p>
                <p>The data load was successful, but the original config and data files
                   may still be in their original locations. Manual cleanup is recommended
                   to prevent duplicate processing if this function is re-run on them.</p>
                </body>
                </html>"""
            #send_email_notifications(email, subject, body, is_error=True, project_id=project_id)

            try:
                # Use fallback email logic
                recipient_email = email if email else FROM_EMAIL
                if not recipient_email:
                    logger.error(f"No recipient email (config or FROM_EMAIL) found. Cannot send cleanup failure warning.")
                else:
                    send_email_notifications(recipient_email, subject, body, is_error=True, project_id=project_id)
            except Exception as email_e:
                logger.error(f"CRITICAL: Failed to send cleanup failure notification: {str(email_e)}")
                logger.error(f"Original file move error was: {str(e)}")
        
        subject = f"BigQuery Data Load Success - {full_table_id}"
        body = f"""<html>
            <body>
            <h2>BigQuery Data Load Completed Successfully</h2>
            <p><strong>Table:</strong> {full_table_id}</p>
            <p><strong>CSV File Path:</strong> {gcs_uri}</p>
            <p><strong>Config File:</strong> {config_file_name}</p>
            <p><strong>Rows Loaded:</strong> {load_job.output_rows}</p>
            <p><strong>Job ID:</strong> {load_job.job_id}</p>
            </body>
            </html>"""
        
        send_email_notifications(email, subject, body, is_error=False, project_id=project_id)
        
        logger.info(f"Successfully completed processing: {config_file_name}")
        
        return 'OK'
        
    except (ConfigValidationError, DataLoadError, FileProcessingError, PermissionError, QuotaExceededError, 
            TableConflictError, InvalidCSVFormatError, MemoryLimitError, GoogleAuthError, FileNotFoundError, ConnectionError, SecretError) as e:
        error_msg = str(e)
        print
        logger.error(f"Error processing the file {config_file_name}: {error_msg}", exc_info=True)
        
        subject = f"BigQuery Data Load Failed - {full_table_id or 'Unknown'}"
        body = f"""<html>
            <body>
            <h2>BigQuery Data Load Failed</h2>
            <p><strong>Error Type:</strong> {type(e).__name__}</p>
            <p><strong>Config File:</strong> {config_file_name or 'Unknown'}</p>
            <p><strong>CSV File Path:</strong> {gcs_uri or 'Unknown'}</p>
            <p><strong>Target Table:</strong> {full_table_id or 'Unknown'}</p>
            <p><strong>Error:</strong> {error_msg}</p>
            <p>Please check the Cloud Function logs for more details.</p>
            </body>
            </html>"""
        
        
        try:
            # Use fallback email logic
            recipient_email = email if email else FROM_EMAIL
            if not recipient_email:
                # Try to load secrets to get FROM_EMAIL if it's not already loaded
                try:
                    _load_secrets(project_id)
                    recipient_email = FROM_EMAIL
                except Exception as load_e:
                    logger.error(f"Could not load secrets to get FROM_EMAIL: {str(load_e)}")
            if not recipient_email:
                logger.error(f"No recipient email (from config or FROM_EMAIL) to send failure notification.")
            else:
                send_email_notifications(recipient_email, subject, body, is_error=True, project_id=project_id)
        except Exception as email_e:
            logger.error(f"CRITICAL: Failed to send failure notification: {str(email_e)}")
            logger.error(f"Original error was: {error_msg}")
        
        return 'OK'
        
    except Exception as e:
        error_msg = str(e)
        logger.error(
            f"Unexpected error processing {config_file_name}: {error_msg}",
            exc_info=True
        )
        
        subject = f"BigQuery Data Load Failed - Unexpected Error"
        body = f"""<html>
            <body>
            <h2>BigQuery Data Load Failed - Unexpected Error</h2>
            <p><strong>Config File:</strong> {config_file_name or 'Unknown'}</p>
            <p><strong>CSV File Path:</strong> {gcs_uri or 'Unknown'}</p>
            <p><strong>Error:</strong> {error_msg}</p>
            <p><strong>Error Type:</strong> {type(e).__name__}</p>
            <p>Please check the Cloud Function logs for full stack trace.</p>
            </body>
            </html>"""
        
        try:
            # Use fallback email logic
            recipient_email = email if email else FROM_EMAIL
            if not recipient_email:
                # Try to load secrets to get FROM_EMAIL if it's not already loaded
                try:
                    _load_secrets(project_id)
                    recipient_email = FROM_EMAIL
                except Exception as load_e:
                    logger.error(f"Could not load secrets to get FROM_EMAIL: {str(load_e)}")

            if not recipient_email:
                logger.error(f"No recipient email (from config or FROM_EMAIL) to send failure notification.")
            else:
                send_email_notifications(recipient_email, subject, body, is_error=True, project_id=project_id)

        except Exception as email_e:
            # CRITICAL: Log the email-sending error, but do not re-raise.
            # This prevents the except block from crashing and causing a retry loop.
            logger.error(f"CRITICAL: Failed to send failure notification: {str(email_e)}")
            logger.error(f"Original error was: {error_msg}")
        
        return 'OK'
