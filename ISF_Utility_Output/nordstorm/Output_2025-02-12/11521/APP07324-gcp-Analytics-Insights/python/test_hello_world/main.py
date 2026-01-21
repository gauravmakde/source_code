import argparse
import os

def get_args():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--m', help='message', required=True)
    return arg_parser.parse_args()

def main():
    args = get_args()
    message = args.m
    env = os.environ.get('ENV', 'local')
    print(f'This code is running in {env} and the message is ::: {message}')

if __name__ == "__main__":
    main()
