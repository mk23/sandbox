#!/bin/bash

DIR=$(date '+%Y%m%d_%H%M')

function finish() {
	export ALL_DONE=23
}

trap finish SIGINT SIGTERM


while [ -z $ALL_DONE ] ; do
	echo "saving to $DIR"
	[ -d "${DIR}" ] || mkdir "${DIR}"
	echo '>>>'
	ls -ld $DIR
	echo '<<<'
	ffmpeg -y -framerate 30 -f avfoundation -video_size 1280x720 -i "1:0" -vf fps=1/30 -q:v 2 "${DIR}/by_%06d.jpg"
done
