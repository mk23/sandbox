#!/bin/bash


if [[ "${1}" =~ ^rtmp[ts]?://([^/]*)/(.*)/(.*)$ ]] ; then
	STREAM_HOST=${BASH_REMATCH[1]}
	STREAM_PATH=${BASH_REMATCH[2]}
	STREAM_NAME=${BASH_REMATCH[3]}
else
	echo "usage: ${0} <rtmp_url>"
	exit 1
fi

LOOKUP_URL="http://just-dnslookup.com"
LOOKUP_URI="index.php?vh=${STREAM_HOST}"

for LOOKUP_URI in $(curl -s "${LOOKUP_URL}/${LOOKUP_URI}" | awk -F"'" '/api\/dnslookupproxy.php/{print $2}' | head -n 5) ; do
	STREAM_LIST="${STREAM_LIST}$(curl -m 5 -s "${LOOKUP_URL}/${LOOKUP_URI}" | grep -v '\(not available\|error\)' | awk -F"::" '/::/{print $1}')"$'\n'
done

for STREAM_ADDR in $(echo "${STREAM_LIST}" | sort | uniq) ; do
	export LD_PRELOAD="$(pwd -P)/libdns_hijack.so.1"
	export HIJACK_HOST="${STREAM_HOST}"
	export HIJACK_ADDR="${STREAM_ADDR}"

	COUNT=$(ffmpeg -i "${1} app=${STREAM_PATH} subscribe=${STREAM_NAME} live=true stop=1000 flashver=9,0,102,16" -acodec copy -vcodec copy -f flv pipe: 2>&1 1>/dev/null | grep -c '\(Video: vp6f\|Audio: mp3\)')

	if [ ${COUNT} -eq 2 ] ; then
		echo ${STREAM_ADDR}:$'\t'good
	else
		echo ${STREAM_ADDR}:$'\t'bad
	fi
done
