#!/usr/bin/python

import logging as log
import httplib, urllib, signal
from xml.etree.cElementTree import fromstring as parseXML, tostring as buildXML, Element

class ConnectError(Exception):
    def __init__(self, xml, *args):
        signal.alarm(0)
        self.args = args
        self.code = xml.find('status').get('code')

        if xml.find('status/invalid') is not None:
            self.subcode = xml.find('status/invalid').get('subcode')

        if len(args) > 0:
            log.debug(*args)
            self.data = args[0] % args[1:]
    def __str__(self):
        return hasattr(self, 'subcode') and '%s/%s' % (self.code, self.subcode) or self.code


class ConnectInfo(object):
    class CurrentUser(object):
        def __init__(self, xml):
            self.id = xml.get('user-id')
            self.name = xml.find('name').text
            self.login = xml.find('login').text

    def __init__(self, xml):
        self.cookie = xml.find('common/cookie').text
        self.version = xml.find('common/version').text
        self.account = xml.find('common/account').get('account-id')

        if xml.find('common/user') is not None:
            self.user = ConnectInfo.CurrentUser(xml.find('common/user'))


class ConnectField(object):
    def __init__(self, xml):
        self.id = xml.get('field-id')
        self.type = xml.get('object-type')
        self.name = xml.find('name').text
        self.info = dict((item.tag, item.text) for item in xml)


class ConnectPrincipal(object):
    def __init__(self, xml):
        self.id = xml.get('principal-id')
        self.type = xml.get('type')
        self.name = xml.find('name').text
        self.info = dict((item.tag, item.text) for item in xml)


class ConnectAPI(object):
    def __init__(self, hostname, username, password, insecure=False, timeout=None):
        # log configuration is a noop if already configured
        log.basicConfig(format='%(asctime)s.%(msecs)03d - %(funcName)10s:%(lineno)-3d - %(levelname)10s: %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S', level=log.DEBUG)

        signal.signal(signal.SIGALRM, self.timeout)

        self.httpconn = insecure and httplib.HTTPConnection or httplib.HTTPSConnection
        log.debug('using %s http connection: %s', insecure and 'insecure' or 'secure', self.httpconn)

        self.hostname = hostname
        self.username = username
        self.password = password
        self.timeout = timeout
        self.session = None
        self.login()

    def timeout(self, *args):
        self.error('request-timeout', 'timeout after %ds processing request: %s', self.timeout, self.uri)

    def request(self, submit=None, **params):
        if self.timeout:
            signal.alarm(self.timeout)

        params['session'] = self.session

        self.uri = '/api/xml?' + urllib.urlencode(dict((k.replace('_', '-'), v) for k, v in params.items() if v is not None))
        log.debug('from %s requesting uri %s', self.hostname, self.uri)

        con = self.httpconn(host=self.hostname, timeout=self.timeout)

        if submit:
            log.debug('using POST with data: %s', submit)
            hdr = {'Content-type': 'application/xml'}
            con.request('POST', self.uri, submit, hdr)
        else:
            log.debug('using GET')
            con.request('GET', self.uri)

        res = con.getresponse()
        log.debug('received response code: %d', res.status)

        if res.status != 200:
            self.error('invalid-http', 'bad http response: %d for request: %s', res.status, self.uri)

        txt = res.read()
        log.debug('received response body: %s', txt.decode('utf8'))

        xml = parseXML(txt)
        if xml.find('status').get('code') != 'ok':
            raise ConnectError(xml, 'invalid xml response: %s for request: %s', xml.find('status').get('code'), self.uri)

        signal.alarm(0)
        return len(xml) == 1 and True or xml

    def error(self, code, *args):
        xml = parseXML('<results><status code="%s"/></results>' % code)
        raise ConnectError(xml, *args)

    def login(self):
        self.session = self.common_info().cookie
        log.debug('extracted connect session id: %s', self.session)

        self.request(action='login', login=self.username, password=self.password)

    def common_info(self):
        return ConnectInfo(self.request(action='common-info'))

    def custom_fields(self):
        xml = self.request(action='custom-fields')
        return [ConnectField(item) for item in xml.find('custom-fields')]

    def principal_info(self, principal_id):
        xml = self.request(action='principal-info', principal_id=principal_id)
        return ConnectPrincipal(xml.find('principal'))

    def principal_list(self, principal_id=None, group_id=None, **filters):
        arg = dict(('filter_' + k, v) for k, v in filters.items() if k in self.principal_list.allowed_filters)
        xml = self.request(action='principal-list', principal_id=principal_id, group_id=group_id, **arg)
        return [ConnectPrincipal(item) for item in xml.find('principal-list')]

    def principal_list_by_field(self, field_id, value):
        xml = self.request(action='principal-list-by-field', field_id=field_id, value=value)
        return [ConnectPrincipal(item) for item in xml.find('principal-list')]
        
    def principal_update(self, **params):
        xml = Element('params')
        xml.append(Element('param', name='action'))
        xml[0].text = 'principal-update'

        for key, val in params.items():
            if key in self.principal_update.allowed_params and val is not None:
                xml.append(Element('param', name=key.replace('_', '-')))
                xml[len(xml) - 1].text = val

        xml = self.request(submit=buildXML(xml))
        return type(xml) == type(Element(None)) and ConnectPrincipal(xml.find('principal')) or xml
        
    def update_group(self, name, login, principal_id=None):
        return self.principal_update(type='group', principal_id=principal_id, name=name, login=login, has_children='true')

    def update_user(self, first_name, last_name, login, password=None, principal_id=None):
        return self.principal_update(type='user', principal_id=principal_id, login=login, email=login,
                                     first_name=first_name, last_name=last_name, password=password,
                                     has_children='false', send_email='false')

    def bulk_action(self, action, data):
        ret = 0
        xml = Element('params')
        while len(data):
            for key, val in data.pop().items():
                xml.append(Element('param', name=key))
                xml[len(xml) - 1].text = val

            if len(xml) >= 30 or len(data) == 0:
                xml.append(Element('param', name='action'))
                xml[len(xml) - 1].text = action

                ret += self.request(submit=buildXML(xml))
                xml.clear()

        return ret

    def group_membership_update(self, principal_id, group_id, is_member='true'):
        return self.request(action='group-membership-update', principal_id=principal_id, group_id=group_id, is_member=is_member)

    def bulk_group_membership_update(self, data):
        return self.bulk_action('group-membership-update', [{'principal-id': p, 'group-id': g, 'is-member': m} for p, g, m in data])

    def acl_field_update(self, acl_id, field_id, value):
        return self.request(action='acl-field-update', acl_id=acl_id, field_id=field_id, value=value)

    def bulk_acl_field_update(self, data):
        return self.bulk_action('acl-field-update', [{'acl-id': a, 'field-id': f, 'value': v} for a, f, v in data])

    principal_list.allowed_filters = (
        'type', 'name', 'email', 'login', 'is_member'
    )
    principal_update.allowed_params = (
        'type', 'principal_id', 'has_children', 'login', 'name', 'first_name', 'last_name', 'password', 'has_children'
    )


if __name__ == '__main__':
    import sys, getpass, optparse

    parser = optparse.OptionParser(usage="%prog [options] <domain>")
    parser.add_option('-q', '--query', default=None,
                      help='query this login instead of the logged in user')
    parser.add_option('-u', '--username', default=None,
                      help='use this username instead of shell user')
    parser.add_option('-p', '--password', default=None,
                      help='use this password instead of prompting')
    parser.add_option('-i', '--insecure', default=False, action='store_true',
                      help='use http instead of https')
    parser.add_option('-t', '--timeout', default=None, type='int',
                      help='abort connection after this many seconds')
    parser.add_option('-v', '--verbose', default=False, action='store_true',
                      help='enable debug mode')

    (opts, args) = parser.parse_args()
    if not opts.username:
        opts.username = getpass.getuser()
    if not opts.password:
        opts.password = getpass.getpass(prompt="password: ")

    if not opts.verbose:
        log.disable(log.DEBUG)

    try:
        breeze = ConnectAPI(args[0], opts.username, opts.password, insecure=opts.insecure, timeout=opts.timeout)
        user = opts.query is not None and opts.query or opts.username

        auth = breeze.common_info()
        print "Auth info"
        print "\tsession: %s ; version: %s ; account: %s" % (auth.cookie, auth.version, auth.account)
        if hasattr(auth, 'user'):
            print "\tauthenticated user: %s (%s) <%s>" % (auth.user.name, auth.user.id, auth.user.login)

        info = breeze.principal_list(login=user)[0]
        print "User info\n",
        print "\t%s: %s (%s)" % (info.type, info.name, info.id)
        if info.info.has_key('email'):
            print "\temail: <%s>" % info.info['email']

        groups = breeze.principal_list(principal_id=info.id, is_member='true')
        print "Group list\n\t", "\n\t".join("%s: %s (%s)" % (i.type, i.name, i.id) for i in groups)
    except IndexError:
        sys.stderr.write("error: invalid url\n")
        parser.print_help()
        sys.exit(1)
