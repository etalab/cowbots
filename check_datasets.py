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


"""Check CKAN-of-Worms datasets for errors in fields and send result to CKAN-of-Worms."""


import argparse
import ConfigParser
import json
import logging
import os
import re
import sys
import urllib2
import urlparse

from biryani1.baseconv import (
    check,
    cleanup_line,
    default,
    empty_to_none,
    function,
    input_to_email,
    make_input_to_url,
    noop,
    not_none,
    pipe,
    struct,
    test,
    test_conv,
    test_equals,
    test_greater_or_equal,
    test_in,
    test_isinstance,
    test_none,
    test_not_in,
    uniform_sequence,
    )
from biryani1.datetimeconv import (
    date_to_iso8601_str,
    datetime_to_iso8601_str,
    iso8601_input_to_date,
    iso8601_input_to_datetime,
    )
from biryani1.jsonconv import (
    make_input_to_json,
    )
from biryani1.states import default_state


app_name = os.path.splitext(os.path.basename(__file__))[0]
conf = None
headers = None
log = logging.getLogger(app_name)
N_ = lambda message: message
name_re = re.compile(ur'[-_\da-z]+$')
slug_re = re.compile(ur'[-\da-z]+$')
uuid_re = re.compile(ur'[\da-f]{8}-[\da-f]{4}-[\da-f]{4}-[\da-f]{4}-[\da-f]{12}$')
year_or_month_or_day_re = re.compile(ur'[0-2]\d{3}(-(0[1-9]|1[0-2])(-([0-2]\d|3[0-1]))?)?$')
year_re = re.compile(ur'(^|[^\d])(19|20)\d\d([^\d]|$)')


# Level-1 Converters


cow_json_to_iso8601_date_str = pipe(
    test_isinstance(basestring),
    iso8601_input_to_date,
    date_to_iso8601_str,
    )

cow_json_to_iso8601_datetime_str = pipe(
    test_isinstance(basestring),
    iso8601_input_to_datetime,
    datetime_to_iso8601_str,
    )

cow_json_to_markdown = pipe(
    test_isinstance(basestring),
    cleanup_line,
    )

cow_json_to_name = pipe(
    test_isinstance(basestring),
    test(lambda name: name == name.strip(), error = N_(u'String begins or ends with spaces')),
    test(lambda name: name == name.strip('-'), error = N_(u'String begins or ends with "-"')),
    test(lambda name: name == name.strip('_'), error = N_(u'String begins or ends with "_"')),
    test(lambda name: '--' not in name, error = N_(u'String contains duplicate "-"')),
    test(lambda name: '__' not in name, error = N_(u'String contains duplicate "_"')),
    test(lambda name: name.islower(), error = N_(u'String must contain only lowercase characters')),
    test(name_re.match, error = N_(u'String must contain only "a"-"z", "0"-"9", "-" & "_"')),
    test(lambda name: len(name) <= 100, error = N_(u'String is too long')),
    )

cow_json_to_tag_name = pipe(
    test_isinstance(basestring),
    test(lambda name: name == name.strip(), error = N_(u'String begins or ends with spaces')),
    test(lambda name: name == name.strip('-'), error = N_(u'String begins or ends with "-"')),
    test(lambda name: '--' not in name, error = N_(u'String contains duplicate "-"')),
    test(lambda name: name.islower(), error = N_(u'String must contain only lowercase characters')),
    test(name_re.match, error = N_(u'String must contain only "a"-"z", "0"-"9" & "-"')),
    test(lambda name: len(name) >= 2, error = N_(u'String is too short')),
    test(lambda name: len(name) <= 100, error = N_(u'String is too long')),
    )

cow_json_to_title = pipe(
    test_isinstance(basestring),
    test(lambda title: title == title.strip(), error = N_(u'String begins or ends with spaces')),
    empty_to_none,
    test(lambda title: not title[0].islower(), error = N_(u'String must begin with an uppercase character')),
    )

cow_json_to_uuid = pipe(
    test_isinstance(basestring),
    test(uuid_re.match, error = N_(u'Invalid ID')),
    )

cow_json_to_year_or_month_or_day_str = pipe(
    test_isinstance(basestring),
    test(year_or_month_or_day_re.match, error = N_(u'Invalid year or month or day')),
    )

cow_response_to_value = pipe(
    make_input_to_json(),
    not_none,
    test_isinstance(dict),
    struct(
        dict(
            apiVersion = pipe(
                test_equals('1.0'),
                not_none,
                ),
            context = noop,
            method = pipe(
                test_isinstance(basestring),
                not_none,
                ),
            params = test_isinstance(dict),
            url = pipe(
                make_input_to_url(full = True),
                not_none,
                ),
            value = noop,
            ),
        ),
    function(lambda response: response['value']),
    )


# Level-2 Converters


cow_json_to_dataset = pipe(
    test_isinstance(dict),
    struct(
        dict(
            draft_id = pipe(
                cow_json_to_uuid,
                not_none,
                ),
            id = pipe(
                cow_json_to_uuid,
                not_none,
                ),
            ),
        default = noop,
        ),
    )

cow_json_to_ids = pipe(
    test_isinstance(list),
    uniform_sequence(
        pipe(
            cow_json_to_uuid,
            not_none,
            ),
        ),
    )

cow_json_to_error_verified_dataset = pipe(
    test_isinstance(dict),
    struct(
        dict(
            license_url = pipe(
                test_isinstance(basestring),
                make_input_to_url(full = True),
                ),
            related = pipe(
                test_isinstance(list),
                uniform_sequence(
                    pipe(
                        test_isinstance(dict),
                        struct(
                            dict(
                                image_url = pipe(
                                    test_isinstance(basestring),
                                    make_input_to_url(full = True),
                                    ),
                                url = pipe(
                                    test_isinstance(basestring),
                                    make_input_to_url(full = True),
                                    ),
                                ),
                            default = noop,
                            ),
                        ),
                    ),
                empty_to_none,
                ),
            resources = pipe(
                test_isinstance(list),
                uniform_sequence(
                    pipe(
                        test_isinstance(dict),
                        struct(
                            dict(
                                url = pipe(
                                    test_isinstance(basestring),
                                    make_input_to_url(full = True),
                                    not_none,
                                    ),
                                ),
                            default = noop,
                            ),
                        ),
                    ),
                empty_to_none,
                not_none,
                ),
            url = pipe(
                test_isinstance(basestring),
                make_input_to_url(full = True),
                ),
            ),
        default = noop,
        ),
    )

cow_json_to_warning_verified_dataset = pipe(
    test_isinstance(dict),
    struct(
        dict(
            alerts = test_isinstance(dict),
            author = cow_json_to_title,
            author_email = input_to_email,
            draft_id = pipe(
                cow_json_to_uuid,
                not_none,
                ),
            extras = pipe(
                test_isinstance(list),
                uniform_sequence(
                    pipe(
                        test_isinstance(dict),
                        struct(
                            dict(
                                key = pipe(
                                    cow_json_to_title,
                                    not_none,
                                    ),
                                value = pipe(
                                    test_isinstance(basestring),
                                    cleanup_line,
                                    not_none,
                                    ),
                                ),
                                default = noop,
                            ),
                        not_none,
                        ),
                    ),
                empty_to_none,
                ),
            frequency = pipe(
                test_isinstance(basestring),
                test_in([
                    u"annuelle",
                    u"aucune",
                    u"bimensuelle",
                    u"bimestrielle",
                    u"hebdomadaire",
                    u"mensuelle",
                    u"ponctuelle",
                    u"quinquennale",
                    u"quotidienne",
                    u"semestrielle",
                    u"temps réel",
                    u"triennale",
                    u"trimestrielle",
                    ], error = N_(u"Invalid frequency")),
                not_none,
                ),
            groups = pipe(
                test_isinstance(list),
                uniform_sequence(
                    pipe(
                        test_isinstance(dict),
                        struct(
                            dict(
                                id = pipe(
                                    cow_json_to_uuid,
                                    not_none,
                                    ),
                                description = cow_json_to_markdown,
                                name = pipe(
                                    cow_json_to_name,
                                    not_none,
                                    ),
                                title = pipe(
                                    cow_json_to_title,
                                    not_none,
                                    ),
                                ),
                            ),
                        not_none,
                        ),
                    ),
                empty_to_none,
                not_none,
                ),
            id = pipe(
                cow_json_to_uuid,
                not_none,
                ),
            isopen = pipe(
                test_isinstance(bool),
                test_equals(True),
                not_none,
                ),
            license_id = pipe(
                test_isinstance(basestring),
                test_in([
                    'cc-by',  # Creative Commons Attribution
                    'cc-by-sa',  # Creative Commons Attribution Share-Alike
                    'cc-zero',  # Creative Commons CCZero
                    'fr-lo',  # Licence Ouverte / Open Licence
                    'odc-by',  # Open Data Commons Attribution License
                    'odc-odbl',  # Open Data Commons Open Database License (ODbL)
                    'odc-pddl',  # Open Data Commons Public Domain Dedication and Licence (PDDL)
                    'other-at',  # Other (Attribution)
                    'other-open',  # Other (Open)
                    'other-pd',  # Other (Public Domain)
                    ]),
                not_none,
                ),
            license_title = pipe(
                test_isinstance(basestring),
                cleanup_line,
                not_none,
                ),
            license_url = noop,  # already tested by error validator
            maintainer = cow_json_to_title,
            maintainer_email = input_to_email,
            metadata_created = pipe(
                cow_json_to_iso8601_date_str,
                not_none,
                ),
            metadata_modified = pipe(
                cow_json_to_iso8601_date_str,
                not_none,
                ),
            name = pipe(
                cow_json_to_name,
                not_none,
                ),
            num_resources = pipe(
                test_isinstance(int),
                test_greater_or_equal(0),
                ),
            num_tags = pipe(
                test_isinstance(int),
                test_greater_or_equal(0),
                ),
            notes = pipe(
                cow_json_to_markdown,
                not_none,
                ),
            organization = test_isinstance(dict),
            owner_org = cow_json_to_uuid,
            private = pipe(
                test_isinstance(bool),
                test_equals(False),
                ),
            related = pipe(
                test_isinstance(list),
                uniform_sequence(
                    pipe(
                        test_isinstance(dict),
                        struct(
                            dict(
                                created = pipe(
                                    cow_json_to_iso8601_datetime_str,
                                    not_none,
                                    ),
                                description = pipe(
                                    cow_json_to_markdown,
                                    not_none,
                                    ),
                                featured = pipe(
                                    test_isinstance(bool),
                                    test_equals(False),
                                    not_none,
                                    ),
                                id = pipe(
                                    cow_json_to_uuid,
                                    not_none,
                                    ),
                                image_url = not_none,  # already tested by error validator
                                owner_id = pipe(
                                    cow_json_to_uuid,
                                    not_none,
                                    ),
                                title = pipe(
                                    cow_json_to_title,
                                    test(lambda title: len(title) >= 8, error = N_(u'String is too short')),
                                    not_none,
                                    ),
                                type = pipe(
                                    test_isinstance(basestring),
                                    cleanup_line,
                                    test_in([
                                        u'api',
                                        u'application',
                                        u'idea',
                                        u'news_article',
                                        u'paper',
                                        u'post',
                                        u'visualization',
                                        ]),
                                    ),
                                url = not_none,  # already tested by error validator
                                view_count = pipe(
                                    test_isinstance(int),
                                    test_greater_or_equal(0),
                                    not_none,
                                    ),
                                ),
                            ),
                        not_none,
                        ),
                    ),
                empty_to_none,
                ),
            relationships_as_object = test_none(),
            relationships_as_subject = test_none(),
            resources = pipe(
                test_isinstance(list),
                uniform_sequence(
                    pipe(
                        test_isinstance(dict),
                        struct(
                            dict(
                                cache_last_updated = test_none(),
                                cache_url = test_none(),
                                created = pipe(
                                    cow_json_to_iso8601_date_str,
                                    not_none,
                                    ),
                                description = pipe(
                                    cow_json_to_markdown,
                                    # not_none,  Currently we accept that only a title be given.
                                    ),
                                format = pipe(
                                    test_isinstance(basestring),
                                    test_conv(
                                        pipe(
                                            function(lambda format: format.upper()),
                                            test_not_in(['KMLZ'], error = N_(u'Invalid format; use "KML" instead')),
                                            test_not_in(['SVGZ'], error = N_(u'Invalid format; use "SVG" instead')),
                                            test_not_in(['XLSX'], error = N_(u'Invalid format; use "XLS" instead')),
                                            test_in([
                                                u'CSV',
                                                u'DOC',
                                                u'DXF',
                                                u'GEOJSON',
                                                u'GML',
                                                u'GPX',
                                                u'GTFS',
                                                u'GZ',
                                                u'HTML',
                                                u'JPG',
                                                u'JSON',
                                                u'KML',
                                                u'MID',
                                                u'MIF',
                                                u'ODS',
                                                u'ODT',
                                                u'PDF',
                                                u'PNG',
                                                u'PPT',
                                                u'RDF',
                                                u'RSS',
                                                u'RTF',
                                                u'SVG',
                                                u'SHP',
                                                u'SQL',
                                                u'TIFF',
                                                u'TXT',
                                                u'WMS',
                                                u'XLS',
                                                u'XML',
                                                u'XSD',
                                                u'WFS',
                                                u'WMS',
                                                u'ZIP',
                                                ]),
                                            ),
                                        ),
                                    # Don't test capitalization, because CKAN uses sometimes upper and lower characters.
                                    # test(lambda format: format == format.upper(),
                                    #     error = N_(u'Format must contain only uppercase characters')),
                                    not_none,
                                    ),
                                hash = test_none(),
                                id = pipe(
                                    cow_json_to_uuid,
                                    not_none,
                                    ),
                                last_modified = cow_json_to_iso8601_date_str,
                                mimetype = test_none(),
                                mimetype_inner = test_none(),
                                name = pipe(
                                    test_isinstance(basestring),
                                    test(lambda title: title == title.strip(),
                                        error = N_(u'String begins or ends with spaces')),
                                    empty_to_none,
                                    not_none,
                                    ),
                                position = pipe(
                                    test_isinstance(int),
                                    test_greater_or_equal(0),
                                    not_none,
                                    ),
                                resource_group_id = pipe(
                                    cow_json_to_uuid,
                                    not_none,
                                    ),
                                resource_type = pipe(
                                    test_isinstance(basestring),
                                    test_in([
                                        u'api',
                                        # u'documentation',
                                        u'file',
                                        u'file.upload',
                                        # u'image',
                                        # u'metadata',
                                        # u'visualization',
                                        ]),
                                    ),
                                revision_id = pipe(
                                    cow_json_to_uuid,
                                    not_none,
                                    ),
                                revision_timestamp = pipe(
                                    cow_json_to_iso8601_datetime_str,
                                    not_none,
                                    ),
                                size = test_none(),
                                state = pipe(
                                    test_isinstance(basestring),
                                    test_equals('active'),
                                    ),
                                tracking_summary = pipe(
                                    test_isinstance(dict),
                                    struct(
                                        dict(
                                            recent = pipe(
                                                test_isinstance(int),
                                                test_greater_or_equal(0),
                                                not_none,
                                                ),
                                            total = pipe(
                                                test_isinstance(int),
                                                test_greater_or_equal(0),
                                                not_none,
                                                ),
                                            ),
                                        ),
                                    not_none,
                                    ),
                                url = noop,  # already tested by error validator
                                webstore_last_updated = test_none(),
                                webstore_url = test_none(),
                                ),
                            ),
                        not_none,
                        ),
                    ),
                empty_to_none,
                # not_none,  # already tested by error validator
                ),
            revision_id = pipe(
                cow_json_to_uuid,
                not_none,
                ),
            revision_timestamp = pipe(
                cow_json_to_iso8601_datetime_str,
                not_none,
                ),
            state = pipe(
                test_isinstance(basestring),
                test_equals('active'),
                ),
            supplier = test_isinstance(dict),
            supplier_id = cow_json_to_uuid,
            tags = pipe(
                test_isinstance(list),
                uniform_sequence(
                    pipe(
                        test_isinstance(dict),
                        struct(
                            dict(
                                name = pipe(
                                    cow_json_to_tag_name,
                                    not_none,
                                    ),
                                ),
                            default = noop,
                            ),
                        not_none,
                        ),
                    ),
                empty_to_none,
                not_none,
                ),
            temporal_coverage_from = cow_json_to_year_or_month_or_day_str,
            temporal_coverage_to = cow_json_to_year_or_month_or_day_str,
            territorial_coverage = pipe(
                test_isinstance(basestring),
                function(lambda value: value.split(',')),
                uniform_sequence(
                    pipe(
                        empty_to_none,
                        test(lambda value: value.count('/') == 1, error = N_(u'Invalid territory')),
                        function(lambda value: value.split('/')),
                        struct(
                            [
                                pipe(
                                    empty_to_none,
                                    test_in(
                                        [
                                            u'ArrondissementOfFrance',
                                            u'AssociatedCommuneOfFrance',
                                            u'CantonalFractionOfCommuneOfFrance',
                                            u'CantonCityOfFrance',
                                            u'CantonOfFrance',
                                            u'CatchmentAreaOfFrance',
                                            u'CommuneOfFrance',
                                            u'Country',
                                            u'DepartmentOfFrance',
                                            u'EmploymentAreaOfFrance',
                                            u'IntercommunalityOfFrance',
                                            u'InternationalOrganization',
                                            u'JusticeAreaOfFrance',
                                            u'MetropoleOfCountry',
                                            u'Mountain',
                                            u'OverseasCollectivityOfFrance',
                                            u'OverseasOfCountry',
                                            u'PaysOfFrance',
                                            u'RegionalNatureParkOfFrance',
                                            u'RegionOfFrance',
                                            u'UrbanAreaOfFrance',
                                            u'UrbanTransportsPerimeterOfFrance',
                                            u'UrbanUnitOfFrance',
                                            ],
                                        error = N_(u'Invalid territory type'),
                                        ),
                                    not_none
                                    ),
                                pipe(
                                    empty_to_none,
                                    not_none
                                    ),
                                ],
                            ),
                        not_none
                        ),
                    ),
                empty_to_none,
                not_none,
                ),
            territorial_coverage_granularity = pipe(
                test_isinstance(basestring),
                test_in([
                    u'canton',
                    u'commune',
                    u'department',
                    u'epci',
                    u'france',
                    u'iris',
                    u'poi',
                    u'region',
                    ]),
                not_none,
                ),
            timestamp = pipe(
                cow_json_to_iso8601_datetime_str,
                not_none,
                ),
            title = pipe(
                cow_json_to_title,
                test(lambda title: len(title) >= 8, error = N_(u'String is too short')),
                test(lambda title: year_re.search(title) is None, error = N_(u'String contains a year')),
                not_none,
                ),
            tracking_summary = test_isinstance(dict),
            type = pipe(
                test_isinstance(basestring),
                test_equals('dataset'),
                ),
            url = noop,  # already tested by error validator
            version = pipe(
                test_isinstance(basestring),
                cleanup_line,
                test_none(),
                ),
            weight = pipe(
                test_isinstance(float),
                test_greater_or_equal(0),
                ),
            ),
        ),
    )


# Functions


def check_dataset(dataset):
    log.debug(u'Checking dataset "{}".'.format(dataset['name']))
    error_verified_dataset, errors = cow_json_to_error_verified_dataset(dataset, state = default_state)
    if errors is None:
        errors = {}
    warning_verified_dataset, warnings = cow_json_to_warning_verified_dataset(error_verified_dataset,
        state = default_state)
    if warnings is None or 'frequency' not in warnings:
        if warning_verified_dataset[u'frequency'] != u'temps réel':
            warning_verified_dataset[u'temporal_coverage_from'], error = not_none(
                warning_verified_dataset[u'temporal_coverage_from'], state = default_state)
            if error is not None:
                if warnings is None:
                    warnings = {}
                warnings[u'temporal_coverage_from'] = error

    alerts = {}
    if errors:
        alerts['error'] = json.loads(json.dumps(errors))  # Convert numeric keys to strings.
    if warnings:
        alerts['warning'] = json.loads(json.dumps(warnings))  # Convert numeric keys to strings.

    if alerts != dict(
            (level, level_alerts[app_name]['error'])
            for level, level_alerts in (dataset.get('alerts') or {}).iteritems()
            if level_alerts.get(app_name)
            ):
        log.info(u'Updating dataset "{}" alerts.'.format(dataset['name']))
        request_headers = headers.copy()
        request_headers['Content-Type'] = 'application/json'
        request = urllib2.Request(urlparse.urljoin(conf['ckan_of_worms.site_url'],
            'api/1/datasets/{}/alert'.format(dataset['id'])), headers = request_headers)
        request_data = dict(
            api_key = conf['ckan_of_worms.api_key'],
            author = app_name,
            draft_id = dataset['draft_id'],
            )
        request_data.update(alerts)
        try:
            response = urllib2.urlopen(request, json.dumps(request_data))
        except urllib2.HTTPError as response:
            if response.code == 409:
                # The dataset has been modified. Don't submit alerts because we will be notified of the new dataset
                #version.
                log.info(u'Dataset "{}" has been modified. Alerts are ignored.'.format(dataset['name']))
                return
            log.error(u'An error occured while setting dataset "{}" alerts: {}'.format(dataset['name'], alerts))
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
            check(cow_response_to_value)(response.read(), state = default_state)


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
    conf = check(pipe(
        test_isinstance(dict),
        struct(
            {
                'ckan_of_worms.api_key': pipe(
                    cleanup_line,
                    not_none,
                    ),
                'ckan_of_worms.site_url': pipe(
                    make_input_to_url(error_if_fragment = True, error_if_path = True, error_if_query = True,
                        full = True),
                    not_none,
                    ),
                'user_agent': pipe(
                    cleanup_line,
                    not_none,
                    ),
                },
            default = 'drop',
            ),
        not_none,
        ))(dict(config_parser.items('CowBots-Check-Datasets')), default_state)

    global headers
    headers = {
        'User-Agent': conf['user_agent'],
        }

    if args.fedmsg:
        import fedmsg

        fedmsg_conf = check(struct(
            dict(
                environment = pipe(
                    empty_to_none,
                    test_in(['dev', 'prod', 'stg']),
                    ),
                modname = pipe(
                    empty_to_none,
                    test(lambda value: value == value.strip('.'), error = 'Value must not begin or end with a "."'),
                    default('ckan_of_worms'),
                    ),
#                name = pipe(
#                    empty_to_none,
#                    default('ckan_of_worms.{}'.format(hostname)),
#                    ),
                topic_prefix = pipe(
                    empty_to_none,
                    test(lambda value: value == value.strip('.'), error = 'Value must not begin or end with a "."'),
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
                    dataset = check(pipe(
                        cow_json_to_dataset,
                        not_none,
                        ))(message['msg'], state = default_state)
                    check_dataset(dataset)
                else:
                    log.debug(u'TODO: Handle {}, {} for {}'.format(kind, action, message))
            else:
                log.debug(u'TODO: Handle {}, {} for {}'.format(kind, action, message))
    else:
        request = urllib2.Request(urlparse.urljoin(conf['ckan_of_worms.site_url'], 'api/1/datasets'), headers = headers)
        response = urllib2.urlopen(request)
        datasets_id = check(pipe(
            cow_response_to_value,
            cow_json_to_ids,
            not_none,
            ))(response.read(), state = default_state)

        for dataset_id in datasets_id:
            request = urllib2.Request(urlparse.urljoin(conf['ckan_of_worms.site_url'],
                'api/1/datasets/{}'.format(dataset_id)), headers = headers)
            response = urllib2.urlopen(request)
            dataset = check(pipe(
                cow_response_to_value,
                cow_json_to_dataset,
                not_none,
                ))(response.read(), state = default_state)
            check_dataset(dataset)

    return 0


if __name__ == '__main__':
    sys.exit(main())
