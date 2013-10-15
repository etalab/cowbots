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


"""Detect changes in CKAN-of-Worms objects and tweet when some patterns are detected."""


import argparse
import collections
import ConfigParser
import logging
import os
import sys
import thread
import time
import urlparse

from biryani1 import baseconv, custom_conv, jsonconv, netconv, states
import twitter


app_name = os.path.splitext(os.path.basename(__file__))[0]
conf = None
conv = custom_conv(baseconv, jsonconv, netconv, states)
headers = None
log = logging.getLogger(app_name)
messages = collections.deque()
next_delays = {
    0: 60,
    60: 300,
    300: 900,
    900: 3600,
    3600: 18000,
    18000: 86400,
    86400: 86400,  # Don't wait more than a day
    }
twitter_api = None


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
#    log.debug(u'Notifying account creation: "{}".'.format(u' - '.join(
#        fragment
#        for fragment in [
#            account.get('fullname'),
#            account.get('name'),
#            account.get('email'),
#            ]
#        if fragment is not None
#        )))
    pass


def dataset_created(dataset):
    log.debug(u'Notifying dataset creation: "{}".'.format(dataset['name']))
    queue_message(
        u'Nouvelles données : {} {}',
        urlparse.urljoin(conf['weckan.site_url'], 'dataset/{}'.format(dataset['name'])),
        dataset['title'],
        )


def group_created(group):
    log.debug(u'Notifying group creation: "{}".'.format(group['name']))
    queue_message(
        u'Nouveau groupe : {} {}',
        urlparse.urljoin(conf['weckan.site_url'], 'group/{}'.format(group['name'])),
        group['title'],
        )


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
                'ckan_of_worms.site_url': conv.pipe(
                    conv.make_input_to_url(error_if_fragment = True, error_if_path = True, error_if_query = True,
                        full = True),
                    conv.not_none,
                    ),
                'twitter.access_token_key': conv.pipe(
                    conv.cleanup_line,
                    conv.not_none,
                    ),
                'twitter.access_token_secret': conv.pipe(
                    conv.cleanup_line,
                    conv.not_none,
                    ),
                'twitter.consumer_key': conv.pipe(
                    conv.cleanup_line,
                    conv.not_none,
                    ),
                'twitter.consumer_secret': conv.pipe(
                    conv.cleanup_line,
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
        ))(dict(config_parser.items('CowBots-Tweet-Changes')), conv.default_state)

    global headers
    headers = {
        'User-Agent': conf['user_agent'],
        }
    global twitter_api
    twitter_api = twitter.Api(
        consumer_key = conf['twitter.consumer_key'],
        consumer_secret = conf['twitter.consumer_secret'],
        access_token_key = conf['twitter.access_token_key'],
        access_token_secret = conf['twitter.access_token_secret'],
        )
    # print twitter_api.VerifyCredentials()
    thread.start_new_thread(tweet_messages, ())

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
                    organization_created(message['msg'])
    else:
        pass  # TODO

    return 0


def organization_created(organization):
    log.debug(u'Notifying organization creation: "{}".'.format(organization['name']))
    queue_message(
        u'Nouvelle organisation : {} {}',
        urlparse.urljoin(conf['weckan.site_url'], 'organization/{}'.format(organization['name'])),
        organization['title'],
        )


def queue_message(template, url, title):
    split_url = urlparse.urlsplit(url)

    # TO remove when site is announced: Remove scheme and network location from URL.
    split_url = list(split_url)
    split_url[0:2] = [u'', u'']
    url = urlparse.urlunsplit(split_url)

    url_length = len(url) if not split_url[1] or split_url[1].startswith(('127.', 'localhost')) \
        else twitter_api.GetShortUrlLength()
    message_length = len(template) - 4 + url_length
    if message_length + len(title) > 140:
        title = title[:140 - message_length - 1] + u'…'
    message = template.format(url, title)
    messages.append(message)
    return message


def tweet_messages():
    delay = 0
    while True:
        if not messages:
            time.sleep(1.0)
            continue
        message = messages.popleft()
        log.info(u'Tweeting: {}'.format(message))
        try:
            log.info(u'    {}'.format(twitter_api.PostUpdate(message)))
        except twitter.TwitterError as e:
            delay = next_delays[delay]
            if 'Too many notices too fast;' in str(e) or 'User is over daily status update limit.' in str(e):
                # Reinject message into queue.
                messages.appendleft(message)
                log.warning(u'Twitter limit reached. Sleeping {}s'.format(delay))
            else:
                log.exception(u'An exception occurred while tweeting: {}. Message lost. Sleeping  {}s'.format(message,
                    delay))
            time.sleep(delay)
        else:
            # Tweet has been sucessfully sent. Reset delay for next tweet.
            delay = 0


if __name__ == '__main__':
    sys.exit(main())
