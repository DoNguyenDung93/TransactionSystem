from datetime import datetime

class Utility:

    @staticmethod
    def get_time_now():
        return Utility.convert_to_datetime_object(str(datetime.utcnow()))

    """
        Converts timestring to specified format.
    """
    @staticmethod
    def convert_to_datetime_object(timestring):
        timestring = timestring[:(timestring.find('.'))]
        return datetime.strptime(timestring, '%Y-%m-%d %H:%M:%S')
