from datetime import datetime

class Utility:

    """
        Converts timestring to specified format.
    """
    @staticmethod
    def convert_to_datetime_object(timestring):
        timestring = timestring[:(timestring.find('.'))]
        return datetime.strptime(timestring, '%Y-%m-%d %H:%M:%S')
