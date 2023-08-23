import os
import shutil

import subprocess
import sys


subprocess.check_call([
    sys.executable, '-m',
    'pip', 'install',
    '--target', './package',
    '-r', 'requirements.txt'
])

os.makedirs('./rds_package/task')
task_dir = f'{os.getcwd()}/task'
for ele in os.listdir(task_dir):
    if ele.endswith('.py'):
        shutil.copy(f'task/{ele}', './rds_package/task')

shutil.make_archive('./rds_package', 'zip', './rds_package')
shutil.rmtree('./rds_package')
