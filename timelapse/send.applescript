#!/usr/bin/env osascript

on run {tgtUser, tgtFile}
    tell application "Messages"
        set tgtSvc to 1st service whose service type = iMessage
        set tgtBdy to buddy tgtUser of tgtSvc
        set tgtMsg to (tgtFile as POSIX file)

        send tgtMsg to tgtBdy
    end tell
end run
