#coding:utf-8

import time
import datetime
import dateutil.parser as dateparser
from threading import Timer
import pytz
import tzlocal

def str_to_timestamp(time_str):

    # OR: dt = time.strptime(datetimestring, fmt)
    # print time_str
    try:
        dt = dateparser.parse(time_str)
        return time.mktime(dt.timetuple())
    except:
        return 0

# datetimestring = 'Fri, 08 Jun 2012 22:40:26 GMT'
# str_to_timestamp(datetimestring)

def current_datetime_string():
    return timestamp_to_str(timestamp_current())

def timestamp_current():
    return int(time.time())

def timestamp_to_str(ts, fmt='%Y-%m-%d %H:%M:%S'):
    return time.strftime(fmt, time.localtime(ts))

def datetime_to_timestamp(dt):
    return int(time.mktime(dt.timetuple()))

def get_across_days(start,end):
    """ 计算 start,end （timestamp) 跨越的日期(天)集合
    """
    s = datetime.datetime.fromtimestamp(start)
    e = datetime.datetime.fromtimestamp(end)
    days = e.date() - s.date()
    result=[]
    s = datetime.datetime(s.year,s.month,s.day)
    for _ in range(days.days+1):
        day = s + datetime.timedelta(_)
        result.append(day)
    return result


class TimedTask(object):
    SECOND = 1
    MINUTE = SECOND * 60

    def __init__(self,action,user_data =None,timeout= SECOND):
        self.action = action
        self.timeout = timeout
        self.start_time = 0
        self.user = user_data
        self.times = 0
        self.timer = None

    def start(self,timeout=0):
        self.start_time = time.time()
        if not timeout:
            timeout =self.timeout
        # if not self.timer:
        self.timer = Timer(self.timeout, self.action, (self,))
        self.timer.start()


    def stop(self):
        self.start_time = 0


def current_date_string():
    return timestamp_to_str(timestamp_current(),fmt='%Y-%m-%d')

def datetime_to_str(dt, fmt='%Y-%m-%d %H:%M:%S'):
    if not dt:
        return ''
    ts = datetime_to_timestamp(dt)
    return time.strftime(fmt, time.localtime(ts))

def localtime2utc(dt = None):
    if not dt:
        dt = datetime.datetime.now()
    return datetime.datetime.utcfromtimestamp( dt.timestamp())

def utctime2local(dt = None):
    if not dt :
        dt = datetime.datetime.utcnow()
    ltz = tzlocal.get_localzone()
    localtime = dt.replace(tzinfo=pytz.utc).astimezone(ltz)
    return  localtime


import functools
# import time

def timer(func):
    """Print the runtime of the decorated function"""
    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        start_time = time.perf_counter()    # 1
        value = func(*args, **kwargs)
        end_time = time.perf_counter()      # 2
        run_time = end_time - start_time    # 3
        print(f"Finished {func.__name__!r} in {run_time:.4f} secs")
        return value
    return wrapper_timer

@timer
def waste_some_time(num_times):
    for _ in range(num_times):
        sum([i**2 for i in range(10000)])
