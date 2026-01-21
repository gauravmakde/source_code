from datetime import datetime
from datetime import timedelta


class RelativeDate:
    def __init__(self, delta=timedelta(days=0)):
        self.date = datetime.today() + delta

    def get_hour_string(self):
        return self.date.strftime('%H')

    def get_day_string(self):
        return self.date.strftime('%d')

    def get_month_string(self):
        return self.date.strftime('%m')

    def get_year_string(self):
        return self.date.strftime('%Y')
