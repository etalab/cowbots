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


"""Detect changes in CKAN-of-Worms objects and notify by email when some patterns are detected."""


import argparse
import ConfigParser
import email.header
import logging
import os
import re
import smtplib
import sys

from biryani1 import baseconv, custom_conv, jsonconv, netconv, states
import mako.lookup


app_dir = os.path.dirname(os.path.abspath(__file__))
app_name = os.path.splitext(os.path.basename(__file__))[0]
conf = None
conv = custom_conv(baseconv, jsonconv, netconv, states)
headers = None
line_re = re.compile(u"""(?P<indent>\s*)(?P<header>([-*]|=>|\[\d+\]|PS\s*\d*\s* ?:)\s*|)(?P<content>[^\s].*)$""")
log = logging.getLogger(app_name)
templates_lookup = None


# Converters


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


# Functions


def account_created(account):
    log.debug(u'Notifying account creation: "{}".'.format(u' - '.join(
        fragment
        for fragment in [
            account.get('fullname'),
            account.get('name'),
            account.get('email'),
            ]
        if fragment is not None
        )))
    template = templates_lookup.get_template('new-account.mako')
    message = template.render_unicode(
        ckan_of_worms_url = conf['ckan_of_worms.site_url'],
        account = account,
        encoding = 'utf-8',
        from_email = conf['from_email'],
        qp = lambda s: to_quoted_printable(s, 'utf-8'),
        to_emails = conf['admin_email'],
        weckan_url = conf['weckan.site_url'],
        ).strip()
    send_email(message)


def dataset_created(dataset):
    log.debug(u'Notifying dataset creation: "{}".'.format(dataset['name']))
    template = templates_lookup.get_template('new-dataset.mako')
    message = template.render_unicode(
        ckan_of_worms_url = conf['ckan_of_worms.site_url'],
        dataset = dataset,
        encoding = 'utf-8',
        from_email = conf['from_email'],
        qp = lambda s: to_quoted_printable(s, 'utf-8'),
        to_emails = conf['admin_email'],
        weckan_url = conf['weckan.site_url'],
        ).strip()
    send_email(message)


def group_created(group):
    log.debug(u'Notifying group creation: "{}".'.format(group['name']))
    template = templates_lookup.get_template('new-group.mako')
    message = template.render_unicode(
        ckan_of_worms_url = conf['ckan_of_worms.site_url'],
        encoding = 'utf-8',
        from_email = conf['from_email'],
        group = group,
        qp = lambda s: to_quoted_printable(s, 'utf-8'),
        to_emails = conf['admin_email'],
        weckan_url = conf['weckan.site_url'],
        ).strip()
    send_email(message)


def main():
    parser = argparse.ArgumentParser(description = __doc__)
    parser.add_argument('config', help = 'path of configuration file')
    parser.add_argument('-f', '--fedmsg', action = 'store_true', help = 'poll fedmsg events')
    parser.add_argument('-v', '--verbose', action = 'store_true', help = 'increase output verbosity')

    global args
    args = parser.parse_args()
    logging.basicConfig(level = logging.DEBUG if args.verbose else logging.WARNING, stream = sys.stdout)

    config_parser = ConfigParser.SafeConfigParser(dict(here = os.path.dirname(args.config)))
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
                            conv.test_email(),
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
                    conv.test_email(),
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
        ))(dict(config_parser.items('CowBots-Email-Changes')), conv.default_state)

    cache_dir = os.path.join(app_dir, 'cache')
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)
    global headers
    headers = {
        'User-Agent': conf['user_agent'],
        }
    global templates_lookup
    templates_lookup = mako.lookup.TemplateLookup(
        directories = [os.path.join(app_dir, 'email-changes-templates')],
        input_encoding = 'utf-8',
        module_directory = os.path.join(cache_dir, 'email-changes-templates'),
        )

    if args.fedmsg:
        import fedmsg

        fedmsg_conf = conv.check(conv.struct(
            dict(
                environment = conv.pipe(
                    conv.empty_to_none,
                    conv.test_in(['dev', 'prod', 'stg']),
                    ),
                modname = conv.pipe(
                    conv.empty_to_none,
                    conv.test(lambda value: value == value.strip('.'), error = 'Value must not begin or end with a "."'),
                    conv.default('ckan_of_worms'),
                    ),
#                name = conv.pipe(
#                    conv.empty_to_none,
#                    conv.default('ckan_of_worms.{}'.format(hostname)),
#                    ),
                topic_prefix = conv.pipe(
                    conv.empty_to_none,
                    conv.test(lambda value: value == value.strip('.'), error = 'Value must not begin or end with a "."'),
                    ),
                ),
            default = 'drop',
            ))(dict(config_parser.items('fedmsg')))

        # Read in the config from /etc/fedmsg.d/.
        fedmsg_config = fedmsg.config.load_config([], None)
        # Disable a warning about not sending.  We know.  We only want to tail.
        fedmsg_config['mute'] = True
        # Disable timing out so that we can tail forever.  This is deprecated
        # and will disappear in future versions.
        fedmsg_config['timeout'] = 0
        # For the time being, don't require message to be signed.
        fedmsg_config['validate_signatures'] = False
        for key, value in fedmsg_conf.iteritems():
            if value is not None:
                fedmsg_config[key] = value

        expected_topic_prefix = '{}.{}.ckan_of_worms.'.format(fedmsg_config['topic_prefix'], fedmsg_config['environment'])
        for name, endpoint, topic, message in fedmsg.tail_messages(**fedmsg_config):
            if not topic.startswith(expected_topic_prefix):
                log.debug(u'Ignoring message: {}, {}'.format(topic, name))
                continue
            kind, action = topic[len(expected_topic_prefix):].split('.')
            if kind == 'account':
                if action == 'create':
                    account_created(message['msg'])
            if kind == 'dataset':
                if action == 'create':
                    dataset_created(message['msg'])
            elif kind == 'group':
                if action == 'create':
                    group_created(message['msg'])
            elif kind == 'organizaton':
                if action == 'create':
                    organizaton_created(message['msg'])
    else:
        pass  # TODO

    return 0


def organization_created(organization):
    log.debug(u'Notifying organization creation: "{}".'.format(organization['name']))
    template = templates_lookup.get_template('new-organization.mako')
    message = template.render_unicode(
        ckan_of_worms_url = conf['ckan_of_worms.site_url'],
        encoding = 'utf-8',
        from_email = conf['from_email'],
        organization = organization,
        qp = lambda s: to_quoted_printable(s, 'utf-8'),
        to_emails = conf['admin_email'],
        weckan_url = conf['weckan.site_url'],
        ).strip()
    send_email(message)


def send_email(message):
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
        server.sendmail(conf['from_email'], conf['admin_email'], message)
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
