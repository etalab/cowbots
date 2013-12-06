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


"""Consume fedmsg messages from CKAN-of-Worms, check their URLs and send result to CKAN-of-Worms."""


import anydbm
import argparse
import ConfigParser
import logging
import os
import sys
import urlparse

from biryani1 import baseconv, custom_conv, jsonconv, states, strings
import mako.lookup
import requests
import requests_oauthlib


app_dir = os.path.dirname(os.path.abspath(__file__))
app_name = os.path.splitext(os.path.basename(__file__))[0]
conf = None
conv = custom_conv(baseconv, jsonconv, states)
db = None
headers = None
log = logging.getLogger(app_name)
oauth = None
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


tumblr_response_to_id = conv.pipe(
    conv.make_input_to_json(),
    conv.not_none,
    conv.test_isinstance(dict),
    conv.struct(
        dict(
            meta = conv.pipe(
                conv.test_isinstance(dict),
                conv.struct(
                    dict(
                        msg = conv.pipe(
                            conv.test_isinstance(basestring),
                            conv.not_none,
                            ),
                        status = conv.pipe(
                            conv.test_isinstance(int),
                            conv.not_none,
                            conv.test_between(200, 299),
                            ),
                        ),
                    default = conv.noop,
                    ),
                conv.not_none,
                ),
            response = conv.pipe(
                conv.test_isinstance(dict),
                conv.struct(
                    dict(
                        id = conv.pipe(
                            conv.test_isinstance(int),
                            conv.not_none,
                            ),
                        ),
                    default = conv.noop,
                    ),
                conv.not_none,
                ),
            ),
        ),
    conv.function(lambda response: response['response']['id']),
    )


# Functions


def dataset_upserted(dataset):
    if not dataset.get('related'):
        return None
    log.debug(u'Updating dataset post in tumbler "{}".'.format(dataset['name']))
    template = templates_lookup.get_template('dataset.mako')
    body = template.render_unicode(
        conf = conf,
        dataset = dataset,
        ).strip()
    post_id_str = db.get(str(dataset['id']))
    if post_id_str is None:
        response = requests.post('https://api.tumblr.com/v2/blog/{}/post'.format(conf['tumblr.hostname']),
            auth = oauth,
            data = dict(
                body = body,
                format = 'html',
                slug = strings.slugify(dataset['name']),
                state = 'published',
                tags = 'opendata,dataviz',
                title = dataset['title'],
                type = 'text',
                ),
            headers = headers,
            )
        post_id = conv.check(conv.pipe(
            tumblr_response_to_id,
            conv.not_none,
            ))(response.text, state = conv.default_state)
        db[str(dataset['id'])] = str(post_id)
    else:
        response = requests.post('https://api.tumblr.com/v2/blog/{}/post/edit'.format(conf['tumblr.hostname']),
            auth = oauth,
            data = dict(
                body = body,
                format = 'html',
                id = int(post_id_str),
                slug = strings.slugify(dataset['name']),
                state = 'published',
                tags = 'opendata,dataviz',
                title = dataset['title'],
                type = 'text',
                ),
            headers = headers,
            )
        post_id = conv.check(conv.pipe(
            tumblr_response_to_id,
            conv.not_none,
            ))(response.text, state = conv.default_state)


def main():
    parser = argparse.ArgumentParser(description = __doc__)
    parser.add_argument('config', help = 'path of configuration file')
    parser.add_argument('-f', '--fedmsg', action = 'store_true', help = 'poll fedmsg events')
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
                'ckan_of_worms.site_url': conv.pipe(
                    conv.make_input_to_url(error_if_fragment = True, error_if_path = True, error_if_query = True,
                        full = True),
                    conv.not_none,
                    ),
                'tumblr.access_token_key': conv.pipe(
                    conv.cleanup_line,
                    conv.not_none,
                    ),
                'tumblr.access_token_secret': conv.pipe(
                    conv.cleanup_line,
                    conv.not_none,
                    ),
                'tumblr.client_key': conv.pipe(
                    conv.cleanup_line,
                    conv.not_none,
                    ),
                'tumblr.client_secret': conv.pipe(
                    conv.cleanup_line,
                    conv.not_none,
                    ),
                'tumblr.hostname': conv.pipe(
                    conv.cleanup_line,
                    conv.not_none,
                    ),
                'user_agent': conv.pipe(
                    conv.cleanup_line,
                    conv.not_none,
                    ),
                },
            default = 'drop',
            ),
        conv.not_none,
        ))(dict(config_parser.items('CowBots-Tumblr-Related')), conv.default_state)

    cache_dir = os.path.join(app_dir, 'cache')
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)
    data_dir = os.path.join(app_dir, 'data')
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    global db
    db = anydbm.open(os.path.join(data_dir, 'tumblr-posts'), 'c')
    global headers
    headers = {
        'User-Agent': conf['user_agent'],
        }
    global templates_lookup
    templates_lookup = mako.lookup.TemplateLookup(
        default_filters = ['h'],
        directories = [os.path.join(app_dir, 'tumblr-related-templates')],
        input_encoding = 'utf-8',
        module_directory = os.path.join(cache_dir, 'tumblr-related-templates'),
        strict_undefined = True,
        )

#    # To obtain access token, uncomment the following code, run it and put the results in configuration file.
#    oauth = requests_oauthlib.OAuth1Session(conf['tumblr.client_key'], client_secret = conf['tumblr.client_secret'])
#    fetch_response = oauth.fetch_request_token('http://www.tumblr.com/oauth/request_token')
#    request_token_key = fetch_response.get('oauth_token')
#    request_token_secret = fetch_response.get('oauth_token_secret')
#    authorization_url = oauth.authorization_url('http://www.tumblr.com/oauth/authorize')
#    print 'Please go here and authorize,', authorization_url

#    redirect_response = raw_input('Paste the full redirect URL here: ')
#    oauth_response = oauth.parse_authorization_response(redirect_response)
#    verifier = oauth_response.get('oauth_verifier')
#    access_token_url = 'http://www.tumblr.com/oauth/access_token'
#    oauth = requests_oauthlib.OAuth1Session(conf['tumblr.client_key'],
#        client_secret = conf['tumblr.client_secret'],
#        resource_owner_key = request_token_key,
#        resource_owner_secret = request_token_secret,
#        verifier = verifier,
#        )
#    oauth_tokens = oauth.fetch_access_token(access_token_url)
#    access_token_key = oauth_tokens.get('oauth_token')
#    print 'access_token_key =', access_token_key
#    access_token_secret = oauth_tokens.get('oauth_token_secret')
#    print 'access_token_secret =', access_token_secret
#    return 0

    global oauth
    oauth = requests_oauthlib.OAuth1(conf['tumblr.client_key'], conf['tumblr.client_secret'],
        conf['tumblr.access_token_key'], conf['tumblr.access_token_secret'])

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
            if kind == 'dataset':
                if action in ('create', 'update'):
                    dataset_upserted(message['msg'])
    else:
        request = requests.get(urlparse.urljoin(conf['ckan_of_worms.site_url'], 'api/1/datasets'),
            params = dict(
                related = 1,
                ),
            headers = headers,
            )
        datasets_id = conv.check(conv.pipe(
            cow_response_to_value,
            conv.not_none,
            ))(request.text, state = conv.default_state)

        for dataset_id in datasets_id:
            response = requests.get(urlparse.urljoin(conf['ckan_of_worms.site_url'],
                'api/1/datasets/{}'.format(dataset_id)), headers = headers)
            dataset = conv.check(conv.pipe(
                cow_response_to_value,
                conv.not_none,
                ))(response.text, state = conv.default_state)
            dataset_upserted(dataset)

    return 0


if __name__ == '__main__':
    sys.exit(main())
