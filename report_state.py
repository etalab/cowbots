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


"""Consume fedmsg messages from CKAN-of-Worms and report changes of states to Dactylo."""


import argparse
import ConfigParser
import json
import logging
import os
import sys
import urllib2
import urlparse

from biryani1 import baseconv, custom_conv, jsonconv, states, strings
import fedmsg


app_name = os.path.splitext(os.path.basename(__file__))[0]
conf = None
conv = custom_conv(baseconv, jsonconv, states)
headers = None
log = logging.getLogger(app_name)
metrics = None
request_headers = None
stats = None


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


def main():
    parser = argparse.ArgumentParser(description = __doc__)
    parser.add_argument('config', help = 'path of configuration file')
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
                'dactylo.api_key': conv.pipe(
                    conv.cleanup_line,
                    conv.not_none,
                    ),
                'dactylo.site_url': conv.pipe(
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
        ))(dict(config_parser.items('CowBots-Report-State')), conv.default_state)

    global headers
    headers = {
        'User-Agent': conf['user_agent'],
        }
    global request_headers
    request_headers = headers.copy()
    request_headers['Content-Type'] = 'application/json'

    request = urllib2.Request(urlparse.urljoin(conf['ckan_of_worms.site_url'], 'api/1/metrics'),
        headers = headers)
    response = urllib2.urlopen(request)
    global metrics
    metrics = conv.check(conv.pipe(
        cow_response_to_value,
        conv.test_isinstance(dict),
        conv.not_none,
        ))(response.read(), state = conv.default_state)

    send_stats()

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
            account = message['msg']
            name = account['name']
            if action == 'create':
                metrics['accounts_count'] += 1
                log.info(u'Updating stats after "{} account {}"'.format(action, name))
                send_stats()
            elif action == 'delete':
                metrics['accounts_count'] -= 1
                log.info(u'Updating stats after "{} account {}"'.format(action, name))
                send_stats()
            elif action != 'update':
                log.warning(u'Unknown action "{}" for account "{}"'.format(action, account))
        elif kind == 'dataset':
            dataset = message['msg']
            name = dataset['name']
            existing_dataset_metrics = metrics['datasets'].get(name, {})
            if action in ('create', 'update'):
                dataset_metrics = dict(
                    organization_name = (dataset.get('organization') or {}).get('name'),
                    related_count = len(dataset.get('related') or []),
                    resources = [
                        dict(
                            format = resource.get('format'),
                            )
                        for resource in (dataset.get('resources') or [])
                        ],
                    weight = dataset['weight'],
                    )
            elif action == 'delete':
                dataset_metrics = None
            else:
                log.warning(u'Unknown action "{}" for dataset "{}"'.format(action, dataset))
                continue
            if dataset_metrics != existing_dataset_metrics:
                log.info(u'Updating stats after "{} dataset {}"'.format(action, name))
                if dataset_metrics is None:
                    del metrics['datasets'][name]
                else:
                    metrics['datasets'][name] = dataset_metrics
                send_stats()
        elif kind == 'organization':
            organization = message['msg']
            name = organization['name']
            existing_organization_metrics = metrics['organizations'].get(name, {})
            if action in ('create', 'update'):
                organization_metrics = dict(
                    public_service = bool(organization.get('public_service')),
                    )
            elif action == 'delete':
                organization_metrics = None
            else:
                log.warning(u'Unknown action "{}" for organization "{}"'.format(action, organization))
                continue
            if organization_metrics != existing_organization_metrics:
                log.info(u'Updating stats after "{} organization {}"'.format(action, name))
                if organization_metrics is None:
                    del metrics['organizations'][name]
                else:
                    metrics['organizations'][name] = organization_metrics
                send_stats()
        else:
            log.debug(u'TODO: Handle {}, {} for {}'.format(kind, action, message))

    return 0


def median(values):
    sorted_values = sorted(values)
    values_count = len(sorted_values)
    quotient, remainder = divmod(values_count, 2)
    if remainder == 0:
        return (sorted_values[quotient - 1] + sorted_values[quotient]) / 2.0
    return sorted_values[quotient]


def median_80_percent(values):
    sorted_values = sorted(values)
    values_count = len(sorted_values)
    quotient, remainder = divmod(values_count, 2)
    if remainder == 0:
        return (sorted_values[quotient - 1] + sorted_values[quotient]) / 2.0
    return sorted_values[quotient]


def send_stats():
    datasets_weight = [
        weight
        for weight in (
            dataset['weight']
            for dataset in metrics['datasets'].itervalues()
            )
        if weight is not None
        ]
    datasets_total_weight = sum(datasets_weight)
    global stats
    stats = dict(
        datasets_average_weight = datasets_total_weight / len(datasets_weight),
        datasets_count = len(metrics['datasets']),
        datasets_median_weight = median(datasets_weight),
        datasets_median_80_percent_weight = median_80_percent(datasets_weight),
        datasets_total_weight = datasets_total_weight,
        formats_count = len(set(
            strings.slugify(resource['format'])
            for dataset in metrics['datasets'].itervalues()
            for resource in dataset['resources']
            )),
        organizations_count = len(metrics['organizations']),
        organizations_public_services_count = sum(
            1
            for organization in metrics['organizations'].itervalues()
            if organization['public_service']
            ),
        related_count = sum(
            dataset['related_count']
            for dataset in metrics['datasets'].itervalues()
            ),
        resources_count = sum(
            len(dataset['resources'])
            for dataset in metrics['datasets'].itervalues()
            ),
        )

    request = urllib2.Request(urlparse.urljoin(conf['dactylo.site_url'], 'api/1/states'), headers = request_headers)
    request_data = dict(
        api_key = conf['dactylo.api_key'],
        value = stats,
        )
    try:
        response = urllib2.urlopen(request, json.dumps(request_data))
    except urllib2.HTTPError as response:
        log.error(u'An error occured while setting stats: {}'.format(stats))
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


if __name__ == '__main__':
    sys.exit(main())
