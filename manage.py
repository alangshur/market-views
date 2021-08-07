import os
import argparse
import pathlib
import sys


# define arguments
parser = argparse.ArgumentParser(description='Utility for starting and stopping background services.')
parser.add_argument('--start', action='store_true', help='Start all background services.')
parser.add_argument('--stop', action='store_true', help='Stop any existing background services.')
parser.add_argument('--clean', action='store_true', help='Clean background service logs and storage dumps.')

# parse arguments
args = parser.parse_args() 
args_count = args.start + args.stop + args.clean
if args_count == 0: parser.error('At least one argument flag must be specified.')
elif args_count > 1: parser.error('Only one argument flag must be specified.')

# check directory
cwd = os.getcwd()
if not cwd.endswith('market-views'):
    parser.error('Must be in root project directory.')
    sys.exit(0)

# create paths
pathlib.Path('redis/').mkdir(parents=True, exist_ok=True)

# start redis
if args.start:
    p_names = os.popen('ps aux').read()
    if 'redis-server' in p_names:
        print('Redis is already started.', flush=True)
    else:
        print('Starting redis... ', end='', flush=True)
        os.chdir('redis')
        os.system('/usr/local/bin/redis-server ../config/redis.conf &> /dev/null')
        os.chdir('..')
        print('Done.', flush=True)

# stop redis
if args.stop:
    p_names = os.popen('ps aux').read()
    if 'redis-server' in p_names:
        print('Stopping redis... ', end='', flush=True)
        os.system("""ps aux | grep -v "grep" | grep "redis-server" | awk '{print $2}' | xargs kill -2""")
        print('Done.', flush=True)
    else:
        print('Redis is not started.', flush=True)

# clean redis logs
if args.clean:
    if os.path.exists('redis/output.log'):
        os.remove('redis/output.log')
    if os.path.exists('redis/appendonly.aof'):
        os.remove('redis/appendonly.aof')
    if os.path.exists('redis/dump.rdb'):
        os.remove('redis/dump.rdb')