import datetime
import logging


def main():
    logging.info(f"[{datetime.datetime.now()}] - Dummy module start")


if __name__ == '__main__':
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    main()