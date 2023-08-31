import os
import shutil

import subprocess
import sys


directory = os.path.abspath(os.path.dirname(__file__))
lambda_name = os.path.basename(directory)
task_dir = f'{directory}/task'
temp_dir = f'{directory}/{lambda_name}_package'
temp_task = f'{temp_dir}/task'
print(f'Creating lambda package: {temp_dir}.zip')

os.makedirs(temp_task, exist_ok=True)

subprocess.check_call([
    sys.executable, '-m',
    'pip', 'install',
    '--target', temp_dir,
    '-r', f'{directory}/requirements.txt'
])

for ele in os.listdir(task_dir):
    if ele.endswith('.py'):
        shutil.copy(f'{task_dir}/{ele}', temp_task)

shutil.make_archive(temp_dir, 'zip', temp_dir)
shutil.rmtree(temp_dir)
print(f'Created lambda package: {temp_dir}.zip')
