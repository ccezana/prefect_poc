import asyncio
import time

import requests
import sys, os
import httpx
from prefect import flow, task
from datetime import datetime
import random
from prefect.deployments import run_deployment

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
STAGED_DIR = os.path.join(PROJECT_ROOT, 'local_dir', 'staged')
INPUT_DIR = os.path.join(PROJECT_ROOT, 'input_dir')
OUTPUT_DIR = os.path.join(PROJECT_ROOT, 'output_dir')
RETURNED_DIR = os.path.join(PROJECT_ROOT, 'local_dir', 'returned')

USERS_DIRS = ['user_1', 'user_2']
SERVERS = ['Alpha', 'Charlie']

PRIORITY_SEP = 1000


@task(log_prints=True)
def is_high_priority(file_path):
    file_size = os.path.getsize(file_path) / 1000
    return True if file_size < PRIORITY_SEP else False


@task(log_prints=True)
def move_file(file, source_dir, destination_dir):
    # files = os.listdir(source_dir)
    if os.path.exists(f'{source_dir}\\{file}'):
        file_name = os.path.basename(file)
        os.rename(f'{source_dir}\\{file}', f'{destination_dir}\\{file_name}')
        print(f"Moved file {file_name} from {source_dir} to {destination_dir}.")
        time.sleep(5)


@task(log_prints=True)
def has_files(directory):
    for file in os.listdir(directory):
        if os.path.isfile(os.path.join(directory, file)):
            return True
    return False


@task(log_prints=True)
def get_subfolders_and_files(directory):
    subfolders_files = {}
    for root, dirs, files in os.walk(directory):
        for subfolder in dirs:
            subfolder_path = os.path.join(root, subfolder)
            subfolders_files[subfolder] = os.listdir(subfolder_path)
    return subfolders_files


@flow(log_prints=True)
def calc_engine_flow(file, user='user_1', cluster='Bravo'):
    move_file(file, f'{STAGED_DIR}\\{user}', f'{INPUT_DIR}\\{cluster}')
    if has_files(f'{INPUT_DIR}\\{cluster}'):
        move_file(file, f'{INPUT_DIR}\\{cluster}', f'{OUTPUT_DIR}\\{cluster}')
        if has_files(f'{OUTPUT_DIR}\\{cluster}'):
            move_file(file, f'{OUTPUT_DIR}\\{cluster}', f'{RETURNED_DIR}\\{user}')


@flow(log_prints=True)
def schedule_detector_test_flow():
    staged_files = get_subfolders_and_files(STAGED_DIR)
    for sub_dir in staged_files:
        user = sub_dir
        user_dir = f'{STAGED_DIR}\\{user}'
        if staged_files[user]:
            files = os.listdir(user_dir)
            for file in files:
                file_path = f'{user_dir}\\{file}'
                if os.path.exists(file_path):
                    work_pool = 'Alpha' if is_high_priority(file_path) else 'Charlie'
                    time_stamp = datetime.now().strftime("%d_%I%M%S")
                    dep_name = f"Calc Engine deployment-{time_stamp}"
                    # await calc_engine_flow(file, user, work_pool)
                    calc_engine_flow.from_source(
                        source="C:\\projects\\prefect_demo",
                        entrypoint="deploy.py:calc_engine_flow"
                    ).deploy(
                        name=dep_name,
                        work_pool_name=work_pool,
                        parameters={"file": file, "user": user, "cluster": work_pool}
                    )
                    run_deployment(f'calc-engine-flow/{dep_name}')
