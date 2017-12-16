#!/usr/bin/env python2.7

import argparse
import datetime
import itertools
import os
import re
import subprocess
import sys
import textwrap

class CustomFormatter(argparse.ArgumentDefaultsHelpFormatter, argparse.RawDescriptionHelpFormatter):
    pass

def package_info(key, cache={}):
    try:
        if not cache:
            lines = subprocess.check_output(['dpkg-parsechangelog'], stderr=open(os.devnull, 'w')).split('\n')
            items = [line.split(':', 1) for line in lines if line and not line.startswith(' ')]
            cache.update(dict((k.lower(), v.strip()) for k, v in items))

        return cache.get(key)
    except (OSError, subprocess.CalledProcessError):
        return ''

def bump_version(suffix='', bump_major=False, bump_minor=False, bump_patch=False, no_bump=False):
    types = {
        'ds': r'(?P<MAJOR>\d{8})\.(?P<PATCH>\d{3})(?P<EXTRA>.*)',
        'mm': r'(?P<MAJOR>\d+)\.(?P<MINOR>\d+)\.(?P<BUILD>\d+)(?:\.(?P<PATCH>\d+))?(?P<EXTRA>.*)',
    }

    for label, regex in types.items():
        match = re.match(regex, package_info('version'))
        if match:
            if label == 'ds':
                today = datetime.date.today().strftime('%Y%m%d')
                major = match.group('MAJOR') if bump_patch else today
                patch = int(match.group('PATCH')) + 1 if bump_patch or match.group('MAJOR') == today else 1

                version = '%s.%03d' % (major, patch)
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
                elif not no_bump:
                    build += 1

                v_arg = [major, minor, build]
                if bump_patch:
                    v_arg.append(patch)

                version = v_fmt % tuple(v_arg) + (suffix.replace('_', '-').replace('~', '+') or match.group('EXTRA') or '')

            if version != package_info('version'):
                return version
            else:
                raise RuntimeError('version not incremented')

    raise RuntimeError('unknown version pattern detected')

def main(argv=sys.argv[1:]):
    parser = argparse.ArgumentParser(description='debian package release helper', formatter_class=CustomFormatter)
    parser.add_argument('-e', '--extra', default=[], nargs=2, action='append', metavar=('FILE', 'REGEX'),
                        help='extra files to update with specified regex with placeholders')
    if bool(package_info('version')):
        action = parser.add_mutually_exclusive_group()
        action.add_argument('-j', '--major', default=False, action='store_true',
                            help='force increment major number')
        action.add_argument('-n', '--minor', default=False, action='store_true',
                            help='force increment minor number')
        action.add_argument('-t', '--patch', default=False, action='store_true',
                            help='force increment patch number')
        action.add_argument('-b', '--no-bump', default=False, action='store_true',
                            help='preserve version, append only')
        action.add_argument('-v', '--version',
                            help='force explicit version number')
        parser.add_argument('-m', '--message', default='Tagging {version}',
                            help='changelog commit message')
        parser.add_argument('-p', '--package', default=package_info('source'),
                            help='package name')
    else:
        parser.add_argument('-m', '--message', default='Tagging initial {version}',
                            help='changelog commit message')
        parser.add_argument('-v', '--version', required=True,
                            help='force explicit version number')
        parser.add_argument('-p', '--package', required=True,
                            help='package name')
    parser.add_argument('-l', '--skiplog', default=False, action='store_true',
                        help='skip adding changelog contents')
    parser.add_argument('-s', '--sources', nargs='+', default=['.'], action='append',
                        help='scm source directories to include in changelog')
    parser.add_argument('-r', '--release', default=package_info('distribution') or 'stable',
                        help='package distribution')
    parser.add_argument('-x', '--extract', nargs=3, metavar=('REGEX', 'NAME', 'URL'),
                        help='commit body issue extractor url with {item} placeholder, issue tracker name, and regex pattern')
    parser.add_argument('-a', '--append', default='',
                        help='append suffix to version string')
    parser.add_argument('-d', '--no-dch', default=False, action='store_true',
                        help='skip updating debian changelog')
    parser.add_argument('-c', '--commit', default=False, action='store_true',
                        help='commit and tag new changelog')
    parser.add_argument('-g', '--sha1-range', default=['{last_sha1}', 'HEAD'], nargs=2, metavar=('START', 'END'),
                        help='git commit range with placeholders')
    parser.add_argument('-f', '--tag-format', default='{package}.{version}',
                        help='git tag format with placeholders')
    parser.epilog = textwrap.dedent('''
        supported placeholders:
            {this_sha1} : latest commit sha1 hash
            {last_sha1} : previous release or specified starting commit sha1 hash
            {tag}       : computed tag string (cannot be used in --tag-format)
            {branch}    : current checked out branch
            {package}   : current source package name
            {version}   : calculated or specified version
    ''')
    args = parser.parse_args(argv)

    strings = {
        'this_sha1' : subprocess.check_output(['git', 'log', '-1', '--format=%H']).strip(),
        'branch'    : subprocess.check_output(['git', 'rev-parse', '--abbrev-ref', 'HEAD']).strip(),
        'version'   : package_info('version'),
        'package'   : args.package or package_info('source'),
    }
    if bool(package_info('version')):
        strings['last_sha1'] = subprocess.check_output(['git', 'rev-list', '--no-merges', '-1', args.tag_format.format(**strings)]).strip()
    else:
        strings['last_sha1'] = subprocess.check_output(['git', 'rev-list', '--max-parents=0', 'HEAD']).strip()
    strings['version']   = args.version or bump_version(args.append.format(**strings), args.major, args.minor, args.patch, args.no_bump)
    strings['tag']       = args.tag_format.format(**strings)

    changed = ['debian/changelog']

    if not args.no_dch:
        if bool(package_info('version')):
            print 'creating changelog entry for %s ...' % strings['version']
            subprocess.check_output(['dch', '--controlmaint', '--force-bad-version', '--newversion', strings['version'], args.message.format(**strings)])
        else:
            print 'creating new changelog for %s ...' % strings['version']
            subprocess.check_output(['dch', '--controlmaint', '--create', '--package', args.package, '--newversion', strings['version'], args.message.format(**strings)])

        if not vars(args).get('skiplog', False):
            git_cmd = ['git', 'log', '--no-merges', '--format=%h %s (%aN)', '%s..%s' % (args.sha1_range[0].format(**strings), args.sha1_range[1].format(**strings))]
            git_cmd.extend(set(itertools.chain.from_iterable(args.sources)))

            changes = subprocess.check_output(git_cmd)
        else:
            changes = ''

        for line in reversed(changes.strip().split('\n')[::-1]):
            if not line:
                continue
            else:
                sha1, text = line.strip().split(' ', 1)

            print '\tappending changelog message for %s ...' % sha1
            subprocess.check_output(['dch', '--controlmaint', '--append', '[%s] %s' % (sha1, text)])

            if args.extract:
                text = subprocess.check_output(['git', 'show', '-s', '--format=%b', sha1])
                bugs = re.findall('%s' % args.extract[0], text, re.IGNORECASE)
                if bugs:
                    subprocess.check_output(['dch', '--controlmaint', '--append', '  %s%s addressed:' % (args.extract[1].capitalize(), 's' if len(bugs) != 1 else '')])
                    for item in bugs:
                        subprocess.check_output(['dch', '--controlmaint', '--append', '    ' + args.extract[2].format(item=item)])


        print 'finalizing changelog release for %s ...' % strings['version']
        subprocess.check_output(['dch', '--maintmaint', '--release', '--force-distribution', '--distribution', args.release, ''])

    for name, patt in args.extra:
        print 'checking %s ...' % name
        part = '(?P<PATTERN>%s)' % patt.format(**dict( (i, '(?P<%s>.*?)' % i) for i in strings.keys()))
        text = open(name).read()
        find = re.search(part, text).groupdict()
        if find:
            print '\tmodifying %s ...' % name
            for key, val in find.items():
                if key in strings:
                    text = text.replace(find['PATTERN'], find['PATTERN'].replace(find[key], strings[key]))

            with open(name, 'w') as data:
                data.write(text)
                changed.append(name)

    if args.commit:
        print 'updating git ...'
        for name in changed:
            print '\tadding changed file to git %s ...' % name
            subprocess.check_output(['git', 'add', name])
        print '\tcommitting changelog to git ...'
        subprocess.check_output(['git', 'commit', '-m', args.message.format(**strings)] + changed)
        print '\ttagging changelog in git ...'
        subprocess.check_output(['git', 'tag', args.tag_format.format(**strings)])
        print 'release prep complete, verify and push the changes and tag'

if __name__ == '__main__':
    main()
