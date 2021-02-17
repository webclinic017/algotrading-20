class DerivativeExpirations():
    """Expriations dates."""

    def __init__(self):
        """Calculate dates."""
        date_list = []
        base_friday = self.closest_friday()
        date_list = self.next_7_fridays(base_friday, date_list)
        date_list = self.find_third_friday(self, 6, date_list)
        date_list = self.find_third_friday(self, 9, date_list)
        date_list = self.january_friday(self, date_list)
        date_list = self.format_dates(date_list)
        self.date_list = date_list

    @classmethod
    def closest_friday(cls):
        """Calculate the closest friday in datetime.date."""
        for d in range(1, 5):
            if (date.today() + timedelta(days=d)).weekday() == 4:
                base_friday = date.today() + timedelta(days=d)
        # print(base_friday)
        return base_friday

    @classmethod
    def next_7_fridays(cls, base_friday, date_list):
        """Get datetime.date for next 7 fridays."""
        for d in range(1, 7):  # Get the next 7 Fridays
            date_list.append(base_friday + timedelta(weeks=d))
        return date_list

    @classmethod
    def find_third_friday(cls, self, month, date_list):
        """Get the third friday of that month."""
        # Month is an integer
        date_list.append(self.third_friday(date.today().year, month))
        return date_list

    @classmethod
    def january_friday(cls, self, date_list):
        """Get January expiration."""
        date_list.append(self.third_friday((date.today() + timedelta(weeks=52)).year, 1))
        date_list.append(self.third_friday((date.today() + timedelta(weeks=104)).year, 1))
        return date_list

    @classmethod
    def format_dates(cls, date_list):
        """Convert dates to strings."""
        date_list = [d.strftime("%Y%m%d") for d in date_list]
        # print(date_list)
        return date_list


    @classmethod
    def third_friday(cls, year, month):
        """Return datetime.date for monthly option expiration given year and
        month
        """
        # The 15th is the lowest third day in the month
        third = datetime.date(year, month, 15)
        # What day of the week is the 15th?
        w = third.weekday()
        # Friday is weekday 4
        if w != 4:
            # Replace just the day (of month)
            third = third.replace(day=(15 + (4 - w) % 7))
        return third
