#!/usr/bin/env python2.7

import argparse
import datetime
import re
import subprocess
import sys

def package_info(key, cache={}):
    try:
        if not cache:
            lines = subprocess.check_output(['dpkg-parsechangelog']).split('\n')
            items = [line.split(': ', 1) for line in lines if line and not line.startswith(' ') and line != 'Changes: ']
            cache.update(dict((k.lower(), v) for k, v in items))

        return cache.get(key)
    except (OSError, subprocess.CalledProcessError):
        return 'UNKNOWN'

def bump_version(bump_major=False, bump_minor=False, bump_patch=False):
    types = {
        'ds': r'(?P<MAJOR>\d{8})\.(?P<PATCH>\d{3})',
        'mm': r'(?P<MAJOR>\d+)\.(?P<MINOR>\d+)\.(?P<BUILD>\d+)(?:\.(?P<PATCH>\d))?',
    }

    for label, regex in types.items():
        match = re.match(regex, package_info('version'))
        if match:
            if label == 'ds':
                today = datetime.date.today().strftime('%Y%m%d')
                major = match.group('MAJOR') if bump_patch else today
                patch = int(match.group('PATCH')) + 1 if bump_patch or match.group('MAJOR') == today else 1

                return '%s.%03d' % (major, patch)
            elif label == 'mm':
                major = int(match.group('MAJOR'))
                minor = int(match.group('MINOR'))
                build = int(match.group('BUILD'))
                patch = int(match.group('PATCH')) if match.group('PATCH') is not None else None
                v_fmt = '%%0%dd.%%0%dd.%%0%dd' % (len(match.group('MAJOR')), len(match.group('MINOR')), len(match.group('BUILD')))
                v_arg = []
                if bump_major:
                    major += 1
                    minor  = 0
                    build  = 0
                elif bump_minor:
                    minor += 1
                    build  = 0
                elif bump_patch:
                    v_fmt += '.%%0%dd' % (len(match.group('PATCH')) if patch is not None else 0)
                    patch  = 1 if patch is None else patch + 1
                else:
                    build += 1

                v_arg = [major, minor, build]
                if bump_patch:
                    v_arg.append(patch)

                print v_fmt, v_arg
                return v_fmt % tuple(v_arg)

    raise RuntimeError('unknown version format detected')

def main(argv=sys.argv[1:]):
    parser = argparse.ArgumentParser(description='debian package release helper', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    action = parser.add_mutually_exclusive_group()
    parser.add_argument('-e', '--extra', default=[], nargs=2, action='append', metavar=('FILE', 'REGEX'),
                        help='extra files to update with specified regex that contain one of {version}, {branch}, or {tag}')
    action.add_argument('-j', '--major', default=False, action='store_true',
                        help='force increment major number')
    action.add_argument('-n', '--minor', default=False, action='store_true',
                        help='force increment minor number')
    action.add_argument('-t', '--patch', default=False, action='store_true',
                        help='force increment patch number')
    action.add_argument('-v', '--version',
                        help='force explicit version number')
    parser.add_argument('-p', '--package', default=package_info('source'),
                        help='package name')
    parser.add_argument('-r', '--release', default=package_info('distribution'),
                        help='package distribution')
    parser.add_argument('-d', '--no-dch', default=False, action='store_true',
                        help='skip updating debian changelog')
    parser.add_argument('-c', '--commit', default=False, action='store_true',
                        help='commit and tag new changelog')
    args = parser.parse_args(argv)

    branch  = subprocess.check_output(['git', 'rev-parse', '--abbrev-ref', 'HEAD']).strip()
    version = args.version or bump_version(args.major, args.minor, args.patch)
    changes = subprocess.check_output(['git', 'log', '--oneline', '%s.%s..HEAD' % (args.package, package_info('version'))]).strip().split('\n')[::-1]
    changed = ['debian/changelog']

    if not args.no_dch:
        print 'creating changelog entry for %s ...' % version
        subprocess.check_output(['dch', '-b', '--newversion', version, 'Tagging %s' % version])

        for line in reversed(changes):
            if not line:
                continue
            else:
                sha1, text = line.strip().split(' ', 1)

            print '\tappending changelog message for %s ...' % sha1
            subprocess.check_output(['dch', '--append', '[%s] %s' % (sha1, text)])

        print 'finalizing changelog release for %s ...' % version
        subprocess.check_output(['dch', '--release', '--distribution', args.release, ''])

    for name, patt in args.extra:
        print 'checking %s ...' % name
        part = '(?P<PATTERN>%s)' % patt.format(version='(?P<VERSION>.*?)', branch='(?P<BRANCH>.*?)', tag='(?P<TAG>.*?)')
        text = open(name).read()
        find = re.search(part, text).groupdict()
        if find:
            print '\tmodifying %s ...' % name
            if 'VERSION' in find:
                text = text.replace(find['PATTERN'], find['PATTERN'].replace(find['VERSION'], version))
            if 'BRANCH' in find:
                text = text.replace(find['PATTERN'], find['PATTERN'].replace(find['BRANCH'], branch))
            if 'TAG' in find:
                text = text.replace(find['PATTERN'], find['PATTERN'].replace(find['TAG'], '%s.%s' % (args.package, version)))

            data = open(name, 'w')
            data.write(text)
            data.close()
            changed.append(name)

    if args.commit:
        print 'updating git ...'
        for name in changed:
            print '\tadding changed file to git %s ...' % name
            subprocess.check_output(['git', 'add', name])
        print '\tcommitting changelog to git ...'
        subprocess.check_output(['git', 'commit', '-m', 'Tagging %s' % version] + changed)
        print '\ttagging changelog in git ...'
        subprocess.check_output(['git', 'tag', '%s.%s' % (args.package, version)])
        print 'release prep complete, verify and push the changes and tag'

if __name__ == '__main__':
    main()
