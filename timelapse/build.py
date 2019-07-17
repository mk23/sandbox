#!/usr/bin/python

import calendar
import datetime
import isodate
import re
import requests
import os
import shutil
import subprocess
import sys
import tempfile

# for i in ../*.jpg ; do ffmpeg -i $i -vf drawtext="text='$(stat -f '%SB' -t '%d/%m/%Y %H\:%M\:%S' $i)': fontfile=/Library/Fonts/AppleGothic.ttf: fontcolor=white@0.8: x=w-tw: y=h-th" txt-$(basename $i) ; done
# ffmpeg -y -r 60 -start_number 1 -i txt-by_%05d.jpg -s 1920x1080 -vcodec libx264 -pix_fmt yuv420p backyard.mp4

os.chdir(sys.path[0])

def fetch_times(date):
    data = {
        'lat': '37.7701',
        'lng': '-121.9188',
#        'date': time.strftime('%Y-%m-%d', time.gmtime(time.mktime(date.timetuple()))),
        'date': date.strftime('%Y-%m-%d'),
        'formatted': '0',
    }

    resp = requests.get('https://api.sunrise-sunset.org/json', params=data).json()['results']
#    dawn = datetime.datetime.fromtimestamp(calendar.timegm(isodate.parse_datetime(resp['astronomical_twilight_begin']).timetuple()))
#    dusk = datetime.datetime.fromtimestamp(calendar.timegm(isodate.parse_datetime(resp['astronomical_twilight_end']).timetuple()))
#    dawn = datetime.datetime.fromtimestamp(calendar.timegm(isodate.parse_datetime(resp['nautical_twilight_begin']).timetuple()))
#    dusk = datetime.datetime.fromtimestamp(calendar.timegm(isodate.parse_datetime(resp['nautical_twilight_end']).timetuple()))
    dawn = datetime.datetime.fromtimestamp(calendar.timegm(isodate.parse_datetime(resp['civil_twilight_begin']).timetuple()))
    dusk = datetime.datetime.fromtimestamp(calendar.timegm(isodate.parse_datetime(resp['civil_twilight_end']).timetuple()))

    return date.date(), dawn, dusk


indx = 0
patt = re.compile(r'\d{8}_\d{4}')
mark = datetime.date.fromtimestamp(0)
temp = tempfile.mkdtemp()
fmtd = '%s/by_%%06d.jpg' % temp
print '>>> created', temp

try:
    if sys.argv[1] == 'today':
        stop = datetime.datetime.today()
    else:
        stop = datetime.datetime.strptime(sys.argv[1], '%Y%m%d')
except:
    stop = None

try:
    for path in filter(lambda x: patt.match(x), sorted(os.listdir('.'), key=os.path.getmtime)):
        for name in sorted(os.listdir(path)):
            date = datetime.datetime.fromtimestamp(os.stat('%s/%s' % (path, name)).st_mtime)

            if stop is not None and date.date() != stop.date():
                print '\033[91m---\033[0m %s/%s (%s != %s)' % (path, name, date, stop)
                continue

            if mark < date.date():
                mark, dawn, dusk = fetch_times(date)

            if date.weekday() < 5 and date > dawn and date < dusk:
#            if date > dawn and date < dusk:
                indx += 1
                print '\033[94m+++\033[0m %s/%s (%s < %s < %s)' % (path, name, dawn, date, dusk)
                subprocess.check_call([
#                print ' '.join([
                    'ffmpeg',
                    '-i', '%s/%s' % (path, name),
                    '-vf', "drawtext=text='%s': fontfile=/Library/Fonts/AppleGothic.ttf: fontcolor=white@0.8: x=w-tw: y=h-th" % date.strftime(r'%b %d %Y %H\:%M'),
                    fmtd % indx,
#                ])
                ], stdout=open(os.devnull, 'w'), stderr=open(os.devnull, 'w'))
            else:
                print '\033[91m---\033[0m %s/%s (%s < %s < %s)' % (path, name, dawn, date, dusk)

    subprocess.check_call([
        'ffmpeg', '-y',
        '-r', '60',
        '-i', fmtd,
        '-s', '1280x720',
        '-vcodec', 'libx264',
        '-pix_fmt', 'yuv420p',
        'backyard.mp4' if stop is None else stop.strftime('%Y%m%d.mp4'),
    ])
finally:
    shutil.rmtree(temp)
    print '<<< removed', temp
