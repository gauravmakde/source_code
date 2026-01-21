from datetime import datetime, timedelta


def daily():
    current_date_time = datetime.now()
    yesterday_date_time = datetime.now() - timedelta(days=1)

    return {'start_date_lte': current_date_time.isoformat(),
            'start_date_gte': yesterday_date_time.isoformat()
            }


def weekly():
    current_date_time = datetime.now()
    yesterday_date_time = datetime.now() - timedelta(days=7)

    return {'start_date_lte': current_date_time.isoformat(),
            'start_date_gte': yesterday_date_time.isoformat()
            }


def days2():
    current_date_time = datetime.now()
    yesterday_date_time = datetime.now() - timedelta(days=2)

    return {'start_date_lte': current_date_time.isoformat(),
            'start_date_gte': yesterday_date_time.isoformat()
            }
