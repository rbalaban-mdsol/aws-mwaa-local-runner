import io
import logging
import os
import platform
import stat
import subprocess
import sys
from datetime import datetime

from airflow.utils.dates import days_ago
from boto3.s3.transfer import S3Transfer
from dateutil import parser as date_parser

import boto3
import yaml
from dotenv import load_dotenv, find_dotenv

from airflow import DAG
from airflow.operators.python import PythonOperator

# Load environment variables from the specified .env file
load_dotenv(find_dotenv())

logging.basicConfig(level=logging.INFO)
def list_s3_files(bucket, prefix):
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if 'Contents' in response:
        return [item['Key'] for item in response['Contents']]
    else:
        return []

def download_s3_file(s3_path, local_path):
    s3 = boto3.client('s3')
    transfer = S3Transfer(s3)
    bucket, key = s3_path.replace("s3://", "").split("/", 1)
    transfer.download_file(bucket, key, local_path)

def upload_s3_file(local_path, s3_path):
    s3 = boto3.client('s3')
    bucket, key = s3_path.replace("s3://", "").split("/", 1)
    s3.upload_file(local_path, bucket, key)

def read_yaml(yaml_file_path):
    with open(yaml_file_path, 'r') as file:
        return yaml.safe_load(file)

def process_shrd_file(s3_file, local_decoder_path, output_bucket):
    # Download the input .shrd file
    input_file_name = os.path.basename(s3_file)
    local_input_path = f"/tmp/{input_file_name}"
    download_s3_file(f"s3://sc-sibel-destination-sandbox/{s3_file}", local_input_path)

    # Create output directory
    output_dir = "./output"
    os.makedirs(output_dir, exist_ok=True)

    # Execute the decoder
    logging.info(f"Executing the decoder on {local_input_path}")

    cfg_file_path = os.getenv('CFG_FILE_PATH', 'cfg.yml')
    logging.info(f"config file path {cfg_file_path}")
    logging.info([local_decoder_path, "-c", local_input_path, "-cfg", f"./dags/{cfg_file_path}", "-o", output_dir])
    files = os.listdir(os.getcwd())
    logging.info(files)
    logging.info(os.path.isfile(f"./dags/{cfg_file_path}"))


    # Decode the output from bytes to string

    # log_info = io.StringIO()
    # log_error = io.StringIO()
    #
    # try:
    #
    #     subprocess.run ([f"./{local_decoder_path}", "-c", local_input_path, "-cfg", f"./dags/{cfg_file_path}", "-o", output_dir],stdout=log_info,stderr=log_error, check=True)
    # except Exception as e:
    #     logging.info(log_info.getvalue())
    #     logging.error(log_error.getvalue())
    #     raise e

    command = [f"./{local_decoder_path}", "-c", local_input_path, "-cfg", f"./dags/{cfg_file_path}", "-o", output_dir]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    stdout, stderr = process.communicate()

    if process.returncode != 0:
        logging.error(f"Subprocess failed with error code: {process.returncode}")
        logging.error(f"Subprocess stderr: {stderr}")
        raise RuntimeError("Subprocess execution failed")

    logging.info(f"Subprocess stdout: {stdout}")
    #process.wait()
    # logging.info( process.stdout.readlines())
    # logging.error(process.stderr.readlines())
    # if process.returncode:
    #     raise RuntimeError("Failed")


    # Initialize variables for site ID and device name
    site_id = "unknown"
    subject_id = "unknown"
    device_name = "unknown"

    # Find and process output files
    output_files = os.listdir(output_dir)
    local_output_path = os.path.join(output_dir, "processed_chest_meta.yaml")
    if os.path.isfile(local_output_path):
        yaml_data = read_yaml(local_output_path)
        site_id = yaml_data.get('site_id', 'unknown')
        subject_id = yaml_data.get('subject_id', 'unknown')
        start_date_time = yaml_data.get('start_date_time', 'unknown')
        device_name = yaml_data.get('device_name', 'unknown')

        try:
            formatted_date_time = date_parser.parse(start_date_time).strftime("%Y-%m-%d_%H-%M-%S")
        except ValueError:
            logging.error(f"Invalid date format in YAML file: {start_date_time}")
            formatted_date_time = "unknown"
    else:
        logging.warning("Yaml file for path creation is not found")

    for output_file in output_files:
        #local_output_path = os.path.join(output_dir, output_file)

        # Construct the S3 path for the CSV file
        file_name = output_file
        local_output_path = os.path.join(output_dir, file_name)
        s3_output_path = f"s3://{output_bucket}/site_id={site_id}/subject_id={subject_id}/formatted_date_time={formatted_date_time}/device_name={device_name}/{file_name}"

        # Upload the CSV file
        logging.info(f"Uploading file to {s3_output_path}")
        upload_s3_file(local_output_path, s3_output_path)

    # Clean up local files
    os.remove(local_input_path)
    for file in os.listdir(output_dir):
        os.remove(os.path.join(output_dir, file))
    os.rmdir(output_dir)

def main():

    input_files_s3_prefix = os.getenv('INPUT_FILE_S3_PATH', 's3://sc-sibel-destination-sandbox/United_States_of_America_Wake Forest/01/2024_04_23_16-05-53_-0400/CJ5907/raw/Chest.shrd')
    decoder_path = os.getenv('DECODER_S3_PATH', 'sensorcloud-lakehouse-sandbox/sibel/decoder/')
    sibel_cli_version = os.getenv('SIBEL_CLI_VERSION', 'linux')

    output_bucket = os.getenv('OUTPUT_BUCKET', 's3://sc-sibel-destination-sandbox/United_States_of_America_Wake Forest/sibeldecoderProcessed')
    decoder_name = os.getenv('DECODER_NAME', 'segno_cli_')
    logging.info("Sibel Decoder: " + decoder_name + sibel_cli_version)

    if not input_files_s3_prefix:
        logging.error("Error: INPUT_FILE_S3_PATH environment variable is not set.")
        sys.exit(1)

    if not decoder_path:
        logging.error("Error: DECODER_S3_PATH environment variable is not set.")
        sys.exit(1)

    if not sibel_cli_version:
        logging.error("Error: SIBEL_CLI_VERSION environment variable is not set.")
        sys.exit(1)

    if not output_bucket:
        logging.error("Error: OUTPUT_BUCKET environment variable is not set.")
        sys.exit(1)

    if not decoder_name:
        logging.error("Error: DECODER_NAME environment variable is not set.")
        sys.exit(1)
    logging.info("Starting compatability test")
    architecture = platform.machine()
    if architecture == 'x86_64':
        sibel_cli_version = 'linux'
    elif architecture in ['arm64', 'aarch64']:
        sibel_cli_version = 'linux'  # Adjust this based on the actual architecture compatibility
    else:
        raise ValueError("Unsupported architecture: " + architecture)

    # Define paths
    base_s3 = decoder_path
    fqdn_decoder = f"s3://{base_s3}{decoder_name}{sibel_cli_version}"
    base_cli_name = f"{decoder_name}{sibel_cli_version}"

    # List available decoders
    list_s3_files("sensorcloud-lakehouse-sandbox", "sibel/decoder/")

    # Download the decoder CLI tool
    logging.info("download started CLI tool")
    local_decoder_path = f"{base_cli_name}"
    logging.info(fqdn_decoder)
    download_s3_file(fqdn_decoder, local_decoder_path)
    logging.info("download successful CLI tool")

    # Make the decoder executable
    # import stat
    # st = os.stat(local_decoder_path)
    # new_permissions = st.st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH
    # os.chmod(local_decoder_path, new_permissions)
    # logging.info("chmod p CLI tool")
    try:
        st = os.stat(local_decoder_path)
        new_permissions = st.st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH
        os.chmod(local_decoder_path, new_permissions)
        logging.info(f"Executable permissions set on CLI tool: {local_decoder_path}")
        logging.info(f"New permissions: {oct(new_permissions)}")
    except OSError as e:
        logging.error(f"Failed to set executable permissions on CLI tool: {str(e)}")
        raise  # Raise the exception to propagate the error up

    # List all raw files to process
    bucket, prefix = input_files_s3_prefix.replace("s3://", "").split("/", 1)
    s3_files = list_s3_files(bucket, prefix)

    # Filter for .shrd files
    shrd_files = [file for file in s3_files if file.endswith('.shrd')]

    for s3_file in shrd_files:
        process_shrd_file(s3_file, local_decoder_path, output_bucket)

    # Clean up decoder file
    os.remove(local_decoder_path)
    logging.info("Process completed successfully.")

# Airflow DAG definition

default_args = {
    'start_date': days_ago(2),
    'catchup': False,
}

dag = DAG(
    'sibel_decoder',
    description='Sibel Decoder DAG',
    schedule_interval="@daily",
    default_args=default_args,
)

hello_operator = PythonOperator(
    task_id='sibel_decoder_task',
    python_callable=main,
    dag=dag
)

