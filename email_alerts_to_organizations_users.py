#! /usr/bin/env python
# -*- coding: utf-8 -*-


# CowBots -- Error detection bots for CKAN-of-Worms
# By: Emmanuel Raviart <emmanuel@raviart.com>
#
# Copyright (C) 2013 Etalab
# http://github.com/etalab/cowbots
#
# This file is part of CowBots.
#
# CowBots is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# CowBots is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


"""Detect alerts in CKAN-of-Worms datasets and notify the users of their organizations by email."""


import argparse
import ConfigParser
import email.header
import logging
import os
import re
import smtplib
import sys
import urllib2
import urlparse

from biryani1 import baseconv, custom_conv, jsonconv, netconv, states
import mako.lookup


app_dir = os.path.dirname(os.path.abspath(__file__))
app_name = os.path.splitext(os.path.basename(__file__))[0]
conf = None
conv = custom_conv(baseconv, jsonconv, netconv, states)
headers = None
line_re = re.compile(u"""(?P<indent>\s*)(?P<header>([-*]|=>|\[\d+\]|PS\s*\d*\s* ?:)\s*|)(?P<content>[^\s].*)$""")
log = logging.getLogger(app_name)
N_ = lambda message: message
templates_lookup = None
uuid_re = re.compile(ur'[\da-f]{8}-[\da-f]{4}-[\da-f]{4}-[\da-f]{4}-[\da-f]{12}$')


# Level-1 Converters


cow_json_to_uuid = conv.pipe(
    conv.test_isinstance(basestring),
    conv.test(uuid_re.match, error = N_(u'Invalid ID')),
    )

cow_response_to_value = conv.pipe(
    conv.make_input_to_json(),
    conv.not_none,
    conv.test_isinstance(dict),
    conv.struct(
        dict(
            apiVersion = conv.pipe(
                conv.test_equals('1.0'),
                conv.not_none,
                ),
            context = conv.noop,
            method = conv.pipe(
                conv.test_isinstance(basestring),
                conv.not_none,
                ),
            params = conv.test_isinstance(dict),
            url = conv.pipe(
                conv.make_input_to_url(full = True),
                conv.not_none,
                ),
            value = conv.noop,
            ),
        ),
    conv.function(lambda response: response['value']),
    )


# Level-2 Converters


cow_json_to_ids = conv.pipe(
    conv.test_isinstance(list),
    conv.uniform_sequence(
        conv.pipe(
            cow_json_to_uuid,
            conv.not_none,
            ),
        ),
    )

cow_json_to_object = conv.pipe(
    conv.test_isinstance(dict),
    conv.struct(
        dict(
            id = conv.pipe(
                cow_json_to_uuid,
                conv.not_none,
                ),
            ),
        default = conv.noop,
        ),
    )


# Functions


def main():
    parser = argparse.ArgumentParser(description = __doc__)
    parser.add_argument('config', help = 'path of configuration file')
    parser.add_argument('-G', '--go', action = 'store_true', default = False,
        help = "send emails to the real email addresses")
    parser.add_argument('-v', '--verbose', action = 'store_true', help = 'increase output verbosity')

    global args
    args = parser.parse_args()
    logging.basicConfig(level = logging.DEBUG if args.verbose else logging.WARNING, stream = sys.stdout)

    config_parser = ConfigParser.SafeConfigParser(dict(
        here = os.path.dirname(os.path.abspath(os.path.normpath(args.config))),
        ))
    config_parser.read(args.config)
    global conf
    conf = conv.check(conv.pipe(
        conv.test_isinstance(dict),
        conv.struct(
            {
                'admin_email': conv.pipe(
                    conv.function(lambda emails: set(emails.split())),
                    conv.uniform_sequence(
                        conv.pipe(
                            conv.input_to_email,
#                            conv.test_email(),
                            ),
                        constructor = lambda emails: sorted(set(emails)),
                        drop_none_items = True,
                        ),
                    conv.empty_to_none,
                    conv.not_none,
                    ),
                'ckan_of_worms.site_url': conv.pipe(
                    conv.make_input_to_url(error_if_fragment = True, error_if_path = True, error_if_query = True,
                        full = True),
                    conv.not_none,
                    ),
                'from_email': conv.pipe(
                    conv.input_to_email,
#                    conv.test_email(),
                    conv.not_none,
                    ),
                'user_agent': conv.pipe(
                    conv.cleanup_line,
                    conv.not_none,
                    ),
                'weckan.site_url': conv.pipe(
                    conv.make_input_to_url(error_if_fragment = True, error_if_path = True, error_if_query = True,
                        full = True),
                    conv.not_none,
                    ),
                },
            default = 'drop',
            ),
        conv.not_none,
        ))(dict(config_parser.items('CowBots-Email-Alerts-To-Organizations-Users')), conv.default_state)

    cache_dir = os.path.join(app_dir, 'cache')
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)
    global headers
    headers = {
        'User-Agent': conf['user_agent'],
        }
    global templates_lookup
    templates_lookup = mako.lookup.TemplateLookup(
        directories = [os.path.join(app_dir, 'email-alerts-to-organizations-users')],
        input_encoding = 'utf-8',
        module_directory = os.path.join(cache_dir, 'email-alerts-to-organizations-users'),
        )

    request = urllib2.Request(urlparse.urljoin(conf['ckan_of_worms.site_url'], 'api/1/organizations'),
        headers = headers)
    response = urllib2.urlopen(request)
    organizations_id = conv.check(conv.pipe(
        cow_response_to_value,
        cow_json_to_ids,
        conv.not_none,
        ))(response.read(), state = conv.default_state)

    for organization_id in organizations_id:
        request = urllib2.Request(urlparse.urljoin(conf['ckan_of_worms.site_url'],
            'api/1/organizations/{}'.format(organization_id)), headers = headers)
        response = urllib2.urlopen(request)
        organization = conv.check(conv.pipe(
            cow_response_to_value,
            cow_json_to_object,
            conv.not_none,
            ))(response.read(), state = conv.default_state)
        log.debug(u'Looking for alerts in: "{}".'.format(organization['title']))

        request = urllib2.Request(urlparse.urljoin(conf['ckan_of_worms.site_url'],
            'api/1/datasets?alerts=error&organization={}'.format(organization_id)), headers = headers)
        response = urllib2.urlopen(request)
        datasets_id = conv.check(conv.pipe(
            cow_response_to_value,
            cow_json_to_ids,
            conv.not_none,
            ))(response.read(), state = conv.default_state)

        datasets = []
        for dataset_id in datasets_id:
            request = urllib2.Request(urlparse.urljoin(conf['ckan_of_worms.site_url'],
                'api/1/datasets/{}'.format(dataset_id)), headers = headers)
            response = urllib2.urlopen(request)
            dataset = conv.check(conv.pipe(
                cow_response_to_value,
                cow_json_to_object,
                conv.not_none,
                ))(response.read(), state = conv.default_state)
            datasets.append(dataset)
        if not datasets:
            continue

        users = [
            user
            for user in (organization.get('users') or [])
            if user.get('capacity') in ('admin', 'editor') and user.get('email') is not None
                and not user['email'].endswith(('@data.gouv.fr', '@etalab2.fr'))
            ]
        if users:
            users_email = [
                user['email']
                for user in users
                ]
            template = templates_lookup.get_template('email.mako')
            message = template.render_unicode(
                ckan_of_worms_url = conf['ckan_of_worms.site_url'],
                datasets = datasets,
                encoding = 'utf-8',
                from_email = conf['from_email'],
                organization = organization,
                qp = lambda s: to_quoted_printable(s, 'utf-8'),
                to_emails = users_email,
                users = users,
                weckan_url = conf['weckan.site_url'],
                ).strip()
            if args.go:
                send_email(users_email + conf['admin_email'], message)
            else:
                send_email(conf['admin_email'], message)
        else:
            # TODO
            pass

    return 0


def send_email(to_emails, message):
    # Rewrap message.
    in_header = True
    message_lines = []
    for line in message.splitlines():
        line = line.rstrip().replace(u' :', u' :').replace(u' [', u' [').replace(u'« ', u'« ').replace(
            u' »', u' »')
        if not line:
            in_header = False
        if in_header or len(line) <= 72:
            message_lines.append(line)
        else:
            match = line_re.match(line)
            assert match is not None
            line_prefix = match.group('indent') + match.group('header')
            line_len = len(line_prefix)
            line_words = []
            for word in match.group('content').split(' '):
                if line_len > len(line_prefix) and line_len + len(word) > 72:
                    message_lines.append(line_prefix + u' '.join(line_words))
                    line_prefix = match.group('indent') + u' ' * len(match.group('header'))
                    line_len = len(line_prefix)
                    line_words = []
                if line_len > 0:
                    line_len += 1
                line_len += len(word)
                line_words.append(word)
            if line_words:
                message_lines.append(line_prefix + u' '.join(line_words))
    message = u'\r\n'.join(message_lines).replace(u' ', u' ').encode('utf-8')
    server = smtplib.SMTP('localhost')
    try:
        server.sendmail(conf['from_email'], to_emails, message)
    except smtplib.SMTPRecipientsRefused:
        log.exception(u'Skipping email to {0}, because an exception occurred:'.format(conf['admin_email']))
    server.quit()


def to_quoted_printable(s, encoding):
    assert isinstance(s, unicode)
    quoted_words = []
    for word in s.split(' '):
        try:
            word = str(word)
        except UnicodeEncodeError:
            word = str(email.header.Header(word.encode(encoding), encoding))
        quoted_words.append(word)
    return ' '.join(quoted_words)


if __name__ == '__main__':
    sys.exit(main())
