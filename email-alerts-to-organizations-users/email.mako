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
import urllib
import urlparse
%>


From: ${from_email}
To: ${u', '.join(to_emails)}
Subject: ${u"[ALERTE data.gouv.fr] Défauts dans les données: {}".format(organization['title']) | qp}
MIME-Version: 1.0
Content-Type: text/plain; charset="${encoding}"

Des défauts ont été signalés sur les jeux de données suivants de l'organization "${organization['title']}".

% for dataset in datasets:
* ${dataset['title']}
% endfor

Vous pouvez voir la liste et le détail de ces défauts dans notre outil de suivi de qualité :
${urlparse.urljoin(ckan_of_worms_url, 'admin/datasets?alerts=error&organization={}'.format(
    urllib.quote_plus(organization['title'].encode('utf-8'))))}

Merci de consacrer quelques instants pour les corriger au plus vite.

Bien cordialement,

L'équipe Etalab
