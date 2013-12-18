## -*- coding: utf-8 -*-


## CowBots -- Error detection bots for CKAN-of-Worms
## By: Emmanuel Raviart <emmanuel@raviart.com>
##
## Copyright (C) 2013 Etalab
## http://github.com/etalab/cowbots
##
## This file is part of CowBots.
##
## CowBots is free software; you can redistribute it and/or modify
## it under the terms of the GNU Affero General Public License as
## published by the Free Software Foundation, either version 3 of the
## License, or (at your option) any later version.
##
## CowBots is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
## GNU Affero General Public License for more details.
##
## You should have received a copy of the GNU Affero General Public License
## along with this program.  If not, see <http://www.gnu.org/licenses/>.


<%!
import urlparse
%>


From: ${from_email}
To: ${u', '.join(to_emails)}
Subject: ${u"[data] Nouvelle réutilisation : {}".format(dataset['title']) | qp}
MIME-Version: 1.0
Content-Type: text/plain; charset="${encoding}"

Une nouvelle réutilisation du jeu de données vient d'être créée :
  ${dataset['title']}

Titre : ${related.get('title') or u''}
Description : ${related.get('description') or u''}
Image : ${related.get('image_url') or u''}
URL : ${related.get('url') or u''}

% if owner is None:
Auteur : Anonyme
% else:
Auteur :
* Nom complet : ${owner.get('fullname') or u''}
* Identifiant de connexion : ${owner.get('name') or u''}
* Courriel : ${owner.get('email') or u''}
% endif

Pour la regarder :
  ${urlparse.urljoin(weckan_url, 'fr/dataset/{}'.format(dataset['name']))}
