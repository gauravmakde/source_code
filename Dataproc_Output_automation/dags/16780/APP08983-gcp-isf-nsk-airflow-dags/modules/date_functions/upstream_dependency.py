from datetime import datetime, timedelta

def day1_query_function():
    current_date_time = datetime.now()
    yesterday_date_time = datetime.now() - timedelta(days=1)

    return {'start_date_lte': current_date_time.isoformat(),
            'start_date_gte': yesterday_date_time.isoformat()
            }
def hours12_query_function():
    current_date_time = datetime.now()
    yesterday_date_time = datetime.now() - timedelta(hours=12)

    return {'start_date_lte': current_date_time.isoformat(),
            'start_date_gte': yesterday_date_time.isoformat()
            }


def weekly_query_function():
    current_date_time = datetime.now()
    yesterday_date_time = datetime.now() - timedelta(days=7)

    return {'start_date_lte': current_date_time.isoformat(),
            'start_date_gte': yesterday_date_time.isoformat()
            }
def days2_query_function():
    current_date_time = datetime.now()
    yesterday_date_time = datetime.now() - timedelta(days=2)

    return {'start_date_lte': current_date_time.isoformat(),
            'start_date_gte': yesterday_date_time.isoformat()
            }
