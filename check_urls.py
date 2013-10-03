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


import argparse
import ConfigParser
import datetime
import json
import logging
import os
import sys
import thread
import time
import urllib2
import urlparse

from biryani1 import baseconv, custom_conv, states
import fedmsg


app_name = os.path.splitext(os.path.basename(__file__))[0]
cache_by_url = {}
conf = None
conv = custom_conv(baseconv, states)
headers = None
log = logging.getLogger(app_name)
pool = set()


# Converters


cow_response_to_value = conv.pipe(
    conv.make_str_to_json(),
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


def check_dataset_urls(dataset, state = None):
    try:
        errors = {}
        url, error = conv.pipe(conv.make_input_to_url(full = True), validate_url)(dataset.get('url'), state = state)
        if error is not None:
            errors['url'] = error

        resources_errors = errors.get('resources') or {}
        for resource_index, resource in enumerate(dataset.get('resources') or []):
            resource_index_str = str(resource_index)
            resource_errors = resources_errors.get(resource_index_str) or {}
            url, error = conv.pipe(conv.make_input_to_url(full = True), validate_url)(resource.get('url'),
                state = state)
            if error is not None:
                resource_errors['url'] = error
            if resource_errors:
                resources_errors[resource_index_str] = resource_errors
            else:
                resources_errors.pop(resource_index_str, None)
        if resources_errors:
            errors['resources'] = resources_errors
        if not errors:
            errors = None

        if ((dataset.get('errors') or {}).get(app_name) or {}).get('error') != errors:
            request_headers = headers.copy()
            request_headers['Content-Type'] = 'application/json'
            request = urllib2.Request(urlparse.urljoin(conf['ckan_of_worms.site_url'],
                'api/1/datasets/{}/errors'.format(dataset['id'])), headers = request_headers)
            try:
                response = urllib2.urlopen(request, json.dumps(dict(
                    api_key = conf['ckan_of_worms.api_key'],
                    author = app_name,
                    draft_id = dataset['draft_id'],
                    value = errors,
                    )))
            except urllib2.HTTPError as response:
                if response.code == 409:
                    # The dataset has been modified. Don't submit errors because we will be notified of the new dataset
                    #version.
                    log.info(u'Dataset "{}" has been modified. Errors are ignored.'.format(dataset['name']))
                    return
                log.error(u'An error occured while setting dataset "{}" errors: {}'.format(dataset['name'], errors))
                response_text = response.read()
                try:
                    response_dict = json.loads(response_text)
                except ValueError:
                    log.error(response_text)
                    raise
                for key, value in response_dict.iteritems():
                    print '{} = {}'.format(key, value)
                raise
            else:
                assert response.code == 200
                conv.check(cow_response_to_value)(response.read(), state = conv.default_state)
    except:
        log.exception(u'An exception occurred for {0}'.format(dataset))
    finally:
        pool.discard(thread.get_ident())


def main():
    parser = argparse.ArgumentParser(description = __doc__)
    parser.add_argument('config', help = 'path of configuration file')
    parser.add_argument('-c', '--thread-count', default = 1, help = 'max number of threads', type = int)
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
                'ckan_of_worms.api_key': conv.pipe(
                    conv.cleanup_line,
                    conv.not_none,
                    ),
                'ckan_of_worms.site_url': conv.pipe(
                    conv.make_input_to_url(error_if_fragment = True, error_if_path = True, error_if_query = True,
                        full = True),
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
        ))(dict(config_parser.items('CowBots-Harvesters')), conv.default_state)

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
#            name = conv.pipe(
#                conv.empty_to_none,
#                conv.default('ckan_of_worms.{}'.format(hostname)),
#                ),
            topic_prefix = conv.pipe(
                conv.empty_to_none,
                conv.test(lambda value: value == value.strip('.'), error = 'Value must not begin or end with a "."'),
                ),
            ),
        default = 'drop',
        ))(dict(config_parser.items('fedmsg')))

    global headers
    headers = {
        'User-Agent': conf['user_agent'],
        }

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
                while len(pool) >= args.thread_count:
                    time.sleep(0.1)
                pool.add(thread.start_new_thread(check_dataset_urls, (message['msg'],)))
            else:
                log.warning(u'TODO: Handle {}, {} for {}'.format(kind, action, message))
        else:
            log.warning(u'TODO: Handle {}, {} for {}'.format(kind, action, message))

    return 0


def set_error(errors, key, (value, error)):
    key_errors = errors.get(key) or []
    if error is None:
        for index, key_error in enumerate(key_errors):
            if key_error.get('author') == app_name:
                del key_errors[index]
                if not key_errors:
                    errors.pop(key, None)
                return True
    else:
        now_str = datetime.datetime.utcnow().isoformat()
        for index, key_error in enumerate(key_errors):
            if key_error.get('author') == app_name:
                if key_error.get('value') == value:
                    if key_error.get('error') == error:
                        return False
                    key_error['error'] = error
                else:
                    key_error.update(dict(
                        created = now_str,
                        error = error,
                        value = value,
                        ))
                return True
        else:
            if not key_errors:
                errors[key] = key_errors
            key_errors.append(dict(
                author = app_name,
                created = now_str,
                error = error,
                value = value,
                ))
            return True
    return False


def validate_url(url, state = None):
    if url is None:
        return None, None
    if state is None:
        state = conv.default_state
    now = datetime.datetime.now()
    refresh = now + datetime.timedelta(minutes = 5)
    url_cache = cache_by_url.get(url)
    if url_cache is not None and url_cache['refresh'] > now:
        log.info(u'Retrieving URL from cache: {}'.format(url))
        return url, url_cache.get('error')
    log.info(u'Checking URL: {}'.format(url))
    request = urllib2.Request(url.encode('utf-8'), headers = headers)
    try:
        response = urllib2.urlopen(request).read()
    except urllib2.HTTPError as response:
        if 200 <= response.code < 400:
            error = state._(u'An error occured when trying to connect to the web server: {0:d} {1}').format(
                response.code, response.msg)
            cache_by_url[url] = dict(error = error, refresh = refresh)
            return url, error
        if response.code not in []:
            error = state._(u'The web server responded with a bad status code: {0:d} {1}').format(response.code,
                response.msg)
            cache_by_url[url] = dict(error = error, refresh = refresh)
            return url, error
    except urllib2.URLError as exception:
        error = state._(u'An error occured when trying to connect to the web server: {0}').format(exception)
        cache_by_url[url] = dict(error = error, refresh = refresh)
        return url, error
    except:
        error = state._(u'An error occured when trying to connect to the web server: {0}').format(sys.exc_info()[0])
        cache_by_url[url] = dict(error = error, refresh = refresh)
        return url, error
    cache_by_url[url] = dict(refresh = refresh)
    return url, None


if __name__ == '__main__':
    sys.exit(main())
