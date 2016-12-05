import os
import platform

home_directory = os.path.expanduser('~')
parent_directory = os.path.abspath(os.path.join(os.getcwd(), os.pardir))
data_directory = os.path.join(home_directory,'news', 'data')
database_directory = os.path.join(parent_directory,'database')
cache_directory = os.path.join(home_directory, 'news', 'cache')
src_files = os.path.join(parent_directory, 'src')
log_directory = '/opt/logs'

python_main_version,python_change_version,python_subchange_version = platform.python_version_tuple()

if not os.path.exists(log_directory):
	os.makedirs(log_directory)

if not os.path.exists(cache_directory):
	os.makedirs(cache_directory)

if not os.path.exists(database_directory):
	os.makedirs(database_directory)

if not os.path.exists(data_directory):
	os.makedirs(data_directory)

if not os.path.exists(os.path.join(src_files, 'data_files')):
	os.makedirs(os.path.join(src_files, 'data_files'))

