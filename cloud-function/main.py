import json
import os
from google.cloud import storage
from google.cloud import bigquery
from google.auth import default
import logging
import functions_framework
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


SENDGRID_API_KEY = os.environ.get('SENDGRID_API_KEY')
FROM_EMAIL = os.environ.get('FROM_EMAIL')
email_flag = True

@functions_framework.cloud_event
def process_config_file(cloud_event):
    """
    Cloud Function that triggers when a config.json file is uploaded to GCS bucket.
    Reads the config file to get file location and loads data into BigQuery.
    
    Args:
        data: The event payload (file metadata)
    """
    data = cloud_event.data
    config_file_name = data['name']
    bucket_name = data['bucket']

    credentials, project_id = default()

    logger.info(f"Project id: {project_id}")
    logger.info(f"Processing config file: {config_file_name} from bucket: {bucket_name}")
    

    try:
        # Initialize clients
        storage_client = storage.Client()
        bq_client = bigquery.Client(project=project_id)
        
        # Get the config file from GCS
        bucket = storage_client.bucket(bucket_name)
        config_blob = bucket.blob(config_file_name)
        
        # Validate that it's a config.json file
        if not config_file_name.lower().endswith('_config.json'):
            logger.warning(f"Skipping non-config file: {config_file_name}")
            return
        
        if "processed/" in config_file_name.lower():
            logger.warning(f"Skipping processed file: {config_file_name}")
            return
        
        
        # Read and parse config file
        data_file_name = config_file_name.split('/')[-1].split('_')[0]
        config_content = config_blob.download_as_text()
        config = json.loads(config_content)
        logger.info(f"Config loaded: {json.dumps(config, indent=2)}")
        
        # Extract required parameters
        file_location = config.get('file_location')
        dataset_id = config.get('dataset')
        email = config.get('email')

        if not file_location:
            raise ValueError("file_location is required in config.json")
        if not email:
            raise ValueError("email is required in config.json")
        if not dataset_id:
            raise ValueError("dataset is required in config.json")
        
        # Extract optional parameters
        override = config.get('override', True)
        tablename = config.get('tablename') or data_file_name
        is_header = config.get('is_header', True)

        logger.info(f"File location: {file_location}")
        logger.info(f"Email: {email}")
        logger.info(f"Dataset: {dataset_id}")
        logger.info(f"Table name: {tablename}")
        logger.info(f"Override: {override}")

        
        # Parse file location (format: gs://bucket-name/path/to/file/ or bucket-name/path/to/file/)
        if file_location.startswith('gs://'):
            file_location = file_location[5:]  # Remove 'gs://' prefix

        parts = file_location.split('/', 1)
        data_bucket_name = parts[0]
        data_file_path = parts[1] if len(parts) > 1 else ""
        
        logger.info(f"Data bucket: {data_bucket_name}")
        logger.info(f"Data file path: {data_file_path}")
        logger.info(f"Data file name: {data_file_name}.csv")
        
        # Get the actual data file from GCS
        data_bucket = storage_client.bucket(data_bucket_name)
        data_blob = data_bucket.blob(f"{data_file_path}{data_file_name}.csv")
        
        if not data_blob.exists():
            raise FileNotFoundError(f"Data file not found: {file_location}")
        
        logger.info(f"Data file exists: {file_location}{data_file_name}.csv")

       
        # Determine table name
        table_name = tablename.replace('-', '_').replace(' ', '_')
        full_table_id = f"{dataset_id}.{table_name}"
        logger.info(f"Target table: {full_table_id}")

        # Build GCS URI
        gcs_uri = f'gs://{data_bucket_name}/{data_file_path}{data_file_name}.csv'
        logger.info(f"Loading data from: {gcs_uri}")

        table_ref = bq_client.dataset(dataset_id).table(table_name)
        skip_leading_rows = 1 if is_header else 0

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=skip_leading_rows,
            autodetect=True,  
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE if override else bigquery.WriteDisposition.WRITE_APPEND
        )
        
                
        # Load data from GCS URI to BigQuery
        load_job = bq_client.load_table_from_uri(
            gcs_uri,
            table_ref,
            job_config=job_config
        )
        
        logger.info(f"Loading data from {gcs_uri} to BigQuery table {full_table_id}...")
        logger.info(f"BigQuery job ID: {load_job.job_id}")
        
        # Wait for job to complete
        load_job.result()
        load_job.reload()
        logger.info(f"BigQuery job state: {load_job.state}")

        if load_job.error_result:
            error_message = load_job.error_result.get('message', 'Unknown error')
            logger.error(f"BigQuery job failed: {error_message}")
            logger.error(f"Error details: {json.dumps(load_job.error_result, indent=2)}")
            if load_job.errors:
                logger.error(f"Job errors: {json.dumps(load_job.errors, indent=2)}")
            raise RuntimeError(f"BigQuery load job {load_job.job_id} failed: {error_message}") #add this gcs-path to dataset.tablname

        logger.info(f"BigQuery load from {gcs_uri} to {full_table_id} completed successfully.")


        # Move data file to processed folder
        processed_path = f"processed/{data_file_name}.csv"
        
        logger.info(f"Moving data file to processed folder: {processed_path}")
        data_bucket.copy_blob(data_blob, data_bucket, processed_path)
        data_blob.delete()
        logger.info(f"Data file moved to: gs://{data_bucket_name}/{processed_path}")

        config_file = os.path.basename(config_file_name)
        processed_config_path = f"processed/{config_file}"
        
        logger.info(f"Moving config file to processed folder: {processed_config_path}")
        bucket.copy_blob(config_blob, bucket, processed_config_path)
        config_blob.delete()
        logger.info(f"Config file moved to: gs://{bucket_name}/{processed_config_path}")


        # Send success email notification
        subject = f"BigQuery Data Load Success - {full_table_id}"
        body = f"""<html>
            <body>
            <h2>BigQuery Data Load Completed Successfully</h2>
            <p><strong>Table:</strong> {full_table_id}</p>
            <p><strong>CSV File Path:</strong> {gcs_uri}</p>
            <p><strong>Config File:</strong> {config_file_name}</p>
            </body>
            </html>"""

        send_email_notifications(email, subject, body)


    except Exception as e:
        error_msg = str(e)
        logger.error(f"Error processing the file {config_file_name}: {error_msg}")
        #raise RuntimeError(f"Cloud Function failed: {error_msg}") from e

        # Send failure email notification
        subject = f"BigQuery Data Load Failed - {full_table_id}"
        body = f"""<html>
            <body>
            <h2>BigQuery Data Load Failed</h2>
            <p><strong>Config File:</strong> {config_file_name}</p>
            <p><strong>CSV File Path:</strong> {gcs_uri}</p>
            <p><strong>Error:</strong> {error_msg}</p>
            <p>Please check the Cloud Function logs for more details.</p>
            </body>
            </html>"""

        send_email_notifications(email, subject, body)



def send_email_notifications(to_email, subject, content):
    """
    Send email notification about the Cloud Function execution status.
    
    Args:
        email: Recipient email address
        subject: Email subject line
        body: Email body content
        status: 'success' or 'error'
    """

    if email_flag:
        if not SENDGRID_API_KEY:
            logger.error("SendGrid API key not set")
            raise ValueError("SendGrid API key is required to send email")
        
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
    
    else:
        logger.info("Skipping email send.")