from datetime import datetime
import importlib
import time
import pytz
import tweepy


def _module_name_to_class(collection):
    module_name = ''
    for pos, char in enumerate(collection):
        if pos != 0:
            module_name = module_name + '_' + char.lower() if char.isupper() else module_name + char
        else:
            module_name = char.lower() if char.isupper() else char

    return module_name


def convert_string_to_collection_class(collection):
    module_name = 'db_collection_structures.' + _module_name_to_class(collection)
    module = importlib.import_module(module_name)
    collection = getattr(module, collection)
    return collection


def limit_handled(cursor):
    while True:
        try:
            yield cursor.next()
        except tweepy.RateLimitError:
            time.sleep(15 * 60)


def timezone_converter(utc_time, time_zone):
    central = utc_time.replace(tzinfo=pytz.utc).astimezone(pytz.timezone(time_zone))
    return central


def formatted_utc_now():
    return datetime.strptime(datetime.utcnow().strftime('%a %b %d %H:%M:%S +0000 %Y'), '%a %b %d %H:%M:%S +0000 %Y')


def formatted_date(dt):
    return datetime.strptime(dt.strftime('%a %b %d %H:%M:%S +0000 %Y'), '%a %b %d %H:%M:%S +0000 %Y')

def formatted_created_at(dt):
    return datetime.strptime(dt, '%a %b %d %H:%M:%S +0000 %Y')
