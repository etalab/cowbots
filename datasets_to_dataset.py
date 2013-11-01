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


"""Retrieve datasets from CKAN (or receive them from fedmsg) and store them in a single dataset."""


import argparse
import ConfigParser
import datetime
import json
import logging
import os
import sys
import urllib
import urllib2
import urlparse

from biryani1 import baseconv, custom_conv, states, strings

from ckantoolbox import ckanconv


app_name = os.path.splitext(os.path.basename(__file__))[0]
conv = custom_conv(baseconv, ckanconv, states)
log = logging.getLogger(app_name)


def main():
    parser = argparse.ArgumentParser(description = __doc__)
    parser.add_argument('config', help = 'path of configuration file')
    parser.add_argument('-d', '--delete', action = 'store_true', help = 'force datastore re-creation')
    parser.add_argument('-f', '--fedmsg', action = 'store_true', help = 'poll fedmsg events')
    parser.add_argument('-v', '--verbose', action = 'store_true', help = 'increase output verbosity')

    global args
    args = parser.parse_args()
    logging.basicConfig(level = logging.DEBUG if args.verbose else logging.WARNING, stream = sys.stdout)

    config_parser = ConfigParser.SafeConfigParser(dict(here = os.path.dirname(args.config)))
    config_parser.read(args.config)
    conf = conv.check(conv.pipe(
        conv.test_isinstance(dict),
        conv.struct(
            {
                'ckan.api_key': conv.pipe(
                    conv.cleanup_line,
                    conv.not_none,
                    ),
                'ckan.site_url': conv.pipe(
                    conv.make_input_to_url(error_if_fragment = True, error_if_path = True, error_if_query = True,
                        full = True),
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
        ))(dict(config_parser.items('CowBots-Datasets-to-Dataset')), conv.default_state)

    ckan_headers = {
        'Authorization': conf['ckan.api_key'],
        'User-Agent': conf['user_agent'],
        }

    organization_name = u'premier-ministre'
    request = urllib2.Request(urlparse.urljoin(conf['ckan.site_url'],
        '/api/3/action/organization_show?id={}'.format(organization_name)), headers = ckan_headers)
    response = urllib2.urlopen(request)
    response_dict = json.loads(response.read())
    organization = conv.check(conv.pipe(
        conv.make_ckan_json_to_organization(),
        conv.not_none,
        ))(response_dict['result'], state = conv.default_state)

    package_title = u'Jeux de données de data.gouv.fr'
    package_name = strings.slugify(package_title)

    # Try to retrieve exising package, to ensure that its resources will not be destroyed by package_update.
    request = urllib2.Request(urlparse.urljoin(conf['ckan.site_url'],
        '/api/3/action/package_show?id={}'.format(package_name)), headers = ckan_headers)
    try:
        response = urllib2.urlopen(request)
    except urllib2.HTTPError as response:
        if response.code == 404:
            package = {}
        else:
            raise
    else:
        assert response.code == 200
        response_dict = json.loads(response.read())
        package = conv.check(conv.pipe(
            conv.make_ckan_json_to_package(drop_none_values = True),
            conv.not_none,
            ))(response_dict['result'], state = conv.default_state)

    package.update(dict(
        author = u'Secrétariat général du Gouvernement',
        author_email = u'bot@etalab2.fr',
        extras = [
            dict(
                key = u"Date de production des données",
                value = datetime.date.today().isoformat(),
                ),
            ],
        frequency = u'temps réel',
        groups = [
            dict(id = strings.slugify(u'État et collectivités')),
            ],
        license_id = u'fr-lo',
        name = package_name,
        notes = u"""Les jeux de données ouvertes collectés par la mission Etalab""",
        owner_org = organization['id'],
#        relationships_as_object (list of relationship dictionaries) – see package_relationship_create() for the format of relationship dictionaries (optional)
#        relationships_as_subject (list of relationship dictionaries) – see package_relationship_create() for the format of relationship dictionaries (optional)
#        resources = [
#            dict(
#                created = entry.get(u'Date de publication'),
#                format = data.get('Format'),
#                last_modified = entry.get(u'Date de dernière modification'),
#                name = data.get('Titre'),
#                # package_id (string) – id of package that the resource needs should be added to.
#                url = data['URL'],
##                        revision_id – (optional)
##                        description (string) – (optional)
##                        hash (string) – (optional)
##                        resource_type (string) – (optional)
##                        mimetype (string) – (optional)
##                        mimetype_inner (string) – (optional)
##                        webstore_url (string) – (optional)
##                        cache_url (string) – (optional)
##                        size (int) – (optional)
##                        cache_last_updated (iso date string) – (optional)
##                        webstore_last_updated (iso date string) – (optional)
#                )
#            for data in entry.get(u'Données', []) + entry.get(u'Documents annexes', [])
#            ],
        state = 'active',  # Undelete package if it was deleted.
#        tags = [
#            dict(
#                name = tag_name,
##                vocabulary_id (string) – the name or id of the vocabulary that the new tag should be added to, e.g. 'Genre'
#                )
#            for tag_name in (
#                strings.slugify(keyword)[:100]
#                for keyword in entry.get(u'Mots-clés', [])
#                if keyword is not None
#                )
#            if len(tag_name) >= 2
#            ],
        territorial_coverage = u'Country/FR',
        territorial_coverage_granularity = u'commune',
        title = package_title,
#        type (string) – the type of the dataset (optional), IDatasetForm plugins associate themselves with different dataset types and provide custom dataset handling behaviour for these types
#        url (string) – a URL for the dataset’s source (optional)
#        version (string, no longer than 100 characters) – (optional)
        ))

    if package.get('id') is None:
        request = urllib2.Request(urlparse.urljoin(conf['ckan.site_url'], '/api/3/action/package_create'),
            headers = ckan_headers)
        try:
            response = urllib2.urlopen(request, urllib.quote(json.dumps(package)))
        except urllib2.HTTPError as response:
            response_text = response.read()
            try:
                response_dict = json.loads(response_text)
            except ValueError:
                log.error(u'An exception occured while creating package: {0}'.format(package))
                log.error(response_text)
                raise
            for key, value in response_dict.iteritems():
                print '{} = {}'.format(key, value)
        else:
            assert response.code == 200
            response_dict = json.loads(response.read())
    else:
        request = urllib2.Request(urlparse.urljoin(conf['ckan.site_url'],
            '/api/3/action/package_update?id={}'.format(package_name)), headers = ckan_headers)
        try:
            response = urllib2.urlopen(request, urllib.quote(json.dumps(package)))
        except urllib2.HTTPError as response:
            response_text = response.read()
            try:
                response_dict = json.loads(response_text)
            except ValueError:
                log.error(u'An exception occured while updating package: {0}'.format(package))
                log.error(response_text)
                raise
            for key, value in response_dict.iteritems():
                print '{} = {}'.format(key, value)
        else:
            assert response.code == 200
            response_dict = json.loads(response.read())
    package = conv.check(conv.pipe(
        conv.make_ckan_json_to_package(),
        conv.not_none,
        ))(response_dict['result'], state = conv.default_state)
    package_id = package['id']

    resources = package['resources']
    if resources:
        assert len(resources) == 1, package
        existing_resource = resources[0].copy()
        existing_resource['package_id'] = package_id
    else:
        existing_resource = {}
    resource = existing_resource.copy()
    resource.update(dict(
        package_id = package_id,
#        format = data.get('Format'),
        name = u"Base de données",
        url = urlparse.urljoin(conf['weckan.site_url'], '/dataset/{}'.format(package_name)),
#        revision_id – (optional)
        description = u"""\
Base de données générée automatiquement à partir du contenu de data.gouv.fr\
""",
#        format (string) – (optional)
#        hash (string) – (optional)
#        resource_type (string) – (optional)
#        mimetype (string) – (optional)
#        mimetype_inner (string) – (optional)
#        webstore_url (string) – (optional)
#        cache_url (string) – (optional)
#        size (int) – (optional)
#        created (iso date string) – (optional)
#        last_modified (iso date string) – (optional)
#        cache_last_updated (iso date string) – (optional)
#        webstore_last_updated (iso date string) – (optional)
        ))
    if resource != existing_resource:
        if resource.get('id') is None:
            request = urllib2.Request(urlparse.urljoin(conf['ckan.site_url'], '/api/3/action/resource_create'),
                headers = ckan_headers)
            try:
                response = urllib2.urlopen(request, urllib.quote(json.dumps(resource)))
            except urllib2.HTTPError as response:
                response_dict = json.loads(response.read())
                for key, value in response_dict.iteritems():
                    print '{} = {}'.format(key, value)
                raise
            else:
                assert response.code == 200
                response_dict = json.loads(response.read())
        else:
            request = urllib2.Request(urlparse.urljoin(conf['ckan.site_url'],
                '/api/3/action/resource_update?id={}'.format(resource['id'])), headers = ckan_headers)
            try:
                response = urllib2.urlopen(request, urllib.quote(json.dumps(resource)))
            except urllib2.HTTPError as response:
                response_dict = json.loads(response.read())
                for key, value in response_dict.iteritems():
                    print '{} = {}'.format(key, value)
                raise
            else:
                assert response.code == 200
                response_dict = json.loads(response.read())
        resource = conv.check(conv.pipe(
            conv.make_ckan_json_to_resource(),
            conv.not_none,
            ))(response_dict['result'], state = conv.default_state)

    if args.delete:
        request = urllib2.Request(urlparse.urljoin(conf['ckan.site_url'], '/api/3/action/datastore_delete'),
            headers = ckan_headers)
        try:
            response = urllib2.urlopen(request, urllib.quote(json.dumps(dict(
                resource_id = resource['id'],
                ))))
        except urllib2.HTTPError as response:
            response_text = response.read()
            try:
                response_dict = json.loads(response_text)
            except ValueError:
                log.error(u'An exception occured while deleting datastore: {0}'.format(resource['id']))
                log.error(response_text)
                raise
            for key, value in response_dict.iteritems():
                print '{} = {}'.format(key, value)
            raise

    for index in range(2):
        datastore = dict(
            fields = [
                dict(id = 'author', type = 'text'),
                dict(id = 'author_email', type = 'text'),
                dict(id = 'ckan_url', type = 'text'),
                dict(id = 'extras', type = 'json'),
                dict(id = 'frequency', type = 'text'),
                dict(id = 'groups', type = 'json'),
                dict(id = 'id', type = 'text'),
#                dict(id = 'isopen', type = 'bool'),
                dict(id = 'license_id', type = 'text'),
#                dict(id = 'license_title', type = 'text'),
#                dict(id = 'license_url', type = 'text'),
                dict(id = 'maintainer', type = 'text'),
                dict(id = 'maintainer_email', type = 'text'),
                dict(id = 'metadata_created', type = 'date'),
                dict(id = 'metadata_modified', type = 'date'),
                dict(id = 'name', type = 'text'),
                dict(id = 'notes', type = 'text'),
                dict(id = 'organization', type = 'json'),
                dict(id = 'owner_org', type = 'text'),
#                dict(id = 'private', type = 'bool'),
                dict(id = 'relationships_as_object', type = 'json'),
                dict(id = 'relationships_as_subject', type = 'json'),
                dict(id = 'resources', type = 'json'),
#                dict(id = 'revision_id', type = 'text'),
                dict(id = 'revision_timestamp', type = 'timestamp'),
#                dict(id = 'state', type = 'text'),
                dict(id = 'supplier', type = 'json'),
                dict(id = 'supplier_id', type = 'text'),
                dict(id = 'tags', type = 'json'),
                dict(id = 'temporal_coverage_from', type = 'text'),  # not a "date", because it can be just a year
                dict(id = 'temporal_coverage_to', type = 'text'),  # not a "date", because it can be just a year
                dict(id = 'territorial_coverage', type = 'text'),
                dict(id = 'territorial_coverage_granularity', type = 'text'),
                dict(id = 'title', type = 'text'),
#                dict(id = 'tracking_summary', type = 'json'),
                dict(id = 'type', type = 'text'),
                dict(id = 'url', type = 'text'),
                dict(id = 'version', type = 'text'),
                ],
            primary_key = 'id',
            resource_id = resource['id'],
            )
        request = urllib2.Request(urlparse.urljoin(conf['ckan.site_url'], '/api/3/action/datastore_create'),
            headers = ckan_headers)
        try:
            response = urllib2.urlopen(request, urllib.quote(json.dumps(datastore)))
        except urllib2.HTTPError as response:
            response_text = response.read()
            try:
                response_dict = json.loads(response_text)
            except ValueError:
                log.error(u'An exception occured while creating datastore: {0}'.format(datastore))
                log.error(response_text)
                raise
            if response.code == 409 and index == 0:
                # Conflict: The fields may have changed. Delete datastore and recreate it.
                request = urllib2.Request(urlparse.urljoin(conf['ckan.site_url'], '/api/3/action/datastore_delete'),
                    headers = ckan_headers)
                try:
                    response = urllib2.urlopen(request, urllib.quote(json.dumps(dict(
                        resource_id = resource['id'],
                        ))))
                except urllib2.HTTPError as response:
                    response_text = response.read()
                    try:
                        response_dict = json.loads(response_text)
                    except ValueError:
                        log.error(u'An exception occured while deleting datastore: {0}'.format(resource['id']))
                        log.error(response_text)
                        raise
                    for key, value in response_dict.iteritems():
                        print '{} = {}'.format(key, value)
                    raise
                continue
            for key, value in response_dict.iteritems():
                print '{} = {}'.format(key, value)
            raise
        assert response.code == 200
        response_dict = json.loads(response.read())
        datastore = conv.check(conv.pipe(
            conv.make_ckan_json_to_datastore(),
            conv.not_none,
            ))(response_dict['result'], state = conv.default_state)
        break

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
                    conv.default('ckan'),
                    ),
#                name = conv.pipe(
#                    conv.empty_to_none,
#                    conv.default('ckan.{}'.format(hostname)),
#                    ),
                topic_prefix = conv.pipe(
                    conv.empty_to_none,
                    conv.test(lambda value: value == value.strip('.'), error = 'Value must not begin or end with a "."'),
                    ),
                ),
            default = 'drop',
            ))(dict(config_parser.items('fedmsg')), state = conv.default_state)

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

        expected_topic_prefix = '{}.{}.ckan.'.format(fedmsg_config['topic_prefix'], fedmsg_config['environment'])
        for name, endpoint, topic, message in fedmsg.tail_messages(**fedmsg_config):
            if not topic.startswith(expected_topic_prefix):
                log.debug(u'Ignoring message: {}, {}'.format(topic, name))
                continue
            kind, action = topic[len(expected_topic_prefix):].split('.')
            if kind == 'package':
                if action in ('create', 'update'):
                    package = conv.check(conv.pipe(
                        conv.make_ckan_json_to_package(drop_none_values = True),
                        conv.not_none,
                        conv.ckan_input_package_to_output_package,
                        ))(message['msg'], state = conv.default_state)
                    if package['id'] == package_id:
                        # Avoid infinite loop.
                        continue
                    log.info(u'Upserting package "{}".'.format(package['name']))
                    assert package.get('ckan_url') is None, package
                    package['ckan_url'] = urlparse.urljoin(conf['weckan.site_url'], '/dataset/{}'.format(package['name']))

                    request = urllib2.Request(urlparse.urljoin(conf['ckan.site_url'], '/api/3/action/datastore_upsert'),
                        headers = ckan_headers)
                    try:
                        response = urllib2.urlopen(request, urllib.quote(json.dumps(dict(
                            method = 'upsert',
                            records = [package],
                            resource_id = resource['id'],
                            ))))
                    except urllib2.HTTPError as response:
                        response_text = response.read()
                        log.error(u'An exception occured while upserting package in datastore: {0}'.format(package))
                        try:
                            response_dict = json.loads(response_text)
                        except ValueError:
                            log.error(response_text)
                            raise
                        for key, value in response_dict.iteritems():
                            print '{} = {}'.format(key, value)
                        raise
                    assert response.code == 200
                    response_dict = json.loads(response.read())
                    assert response_dict['success'] is True
                    # upsert = response_dict['result']
                elif action == 'delete':
                    package = message['msg']  # contains only "id".
                    if package['id'] == package_id:
                        # Avoid infinite loop.
                        continue
                    log.info(u'Deleting package "{}".'.format(package))
                    request = urllib2.Request(urlparse.urljoin(conf['ckan.site_url'], '/api/3/action/datastore_delete'),
                        headers = ckan_headers)
                    try:
                        response = urllib2.urlopen(request, urllib.quote(json.dumps(dict(
                            filters = dict(
                                id = package['id'],
                                ),
                            resource_id = resource['id'],
                            ))))
                    except urllib2.HTTPError as response:
                        if response.code != 404:
                            response_text = response.read()
                            log.error(u'An exception occured while deleting package from datastore: {0}'.format(package))
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
                        response_dict = json.loads(response.read())
                        assert response_dict['success'] is True
                        # delete = response_dict['result']
                else:
                    log.debug(u'TODO: Handle {}, {} for {}'.format(kind, action, message))
            else:
                log.debug(u'TODO: Handle {}, {} for {}'.format(kind, action, message))
    else:
        # Retrieve names of packages already existing in CKAN.
        request = urllib2.Request(urlparse.urljoin(conf['ckan.site_url'], '/api/3/action/package_list'),
            headers = ckan_headers)
        response = urllib2.urlopen(request)
        response_dict = json.loads(response.read())
        packages_name = conv.check(conv.pipe(
            conv.ckan_json_to_name_list,
            conv.not_none,
            ))(response_dict['result'], state = conv.default_state)

        for package_name in packages_name:
            request = urllib2.Request(urlparse.urljoin(conf['ckan.site_url'],
                '/api/3/action/package_show?id={}'.format(package_name)), headers = ckan_headers)
            response = urllib2.urlopen(request)
            response_dict = json.loads(response.read())
            package = conv.check(conv.pipe(
                conv.make_ckan_json_to_package(drop_none_values = True),
                conv.not_none,
                conv.ckan_input_package_to_output_package,
                ))(response_dict['result'], state = conv.default_state)
            assert package.get('ckan_url') is None, package
            package['ckan_url'] = urlparse.urljoin(conf['weckan.site_url'], '/dataset/{}'.format(package['name']))

            request = urllib2.Request(urlparse.urljoin(conf['ckan.site_url'], '/api/3/action/datastore_upsert'),
                headers = ckan_headers)
            try:
                response = urllib2.urlopen(request, urllib.quote(json.dumps(dict(
                    method = 'upsert',
                    records = [package],
                    resource_id = resource['id'],
                    ))))
            except urllib2.HTTPError as response:
                response_text = response.read()
                log.error(u'An exception occured while upserting package in datastore: {0}'.format(package))
                try:
                    response_dict = json.loads(response_text)
                except ValueError:
                    log.error(response_text)
                    raise
                for key, value in response_dict.iteritems():
                    print '{} = {}'.format(key, value)
                raise
            assert response.code == 200
            response_dict = json.loads(response.read())
            assert response_dict['success'] is True
            # upsert = response_dict['result']

    return 0


if __name__ == '__main__':
    sys.exit(main())
