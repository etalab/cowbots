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
Subject: ${u"[data] Nouveau groupe : {}".format(group['title']) | qp}
MIME-Version: 1.0
Content-Type: text/plain; charset="${encoding}"

Salut,

Juste un petit mot pour te signaler qu'un nouveau groupe vient d'être créé :
  ${group['title']}

Pour le regarder :
  ${urlparse.urljoin(weckan_url, 'group/{}'.format(group['name']))}

Si tu n'es pas débordé par tes rendez-vous, tes réunions, tes formations, tes rapports, tes courriels, tes coups de fils, la réorganisation en cours, le prochain déménagement, etc, tu peux aussi faire ton boulot et surveiller son contenu :
  ${urlparse.urljoin(ckan_of_worms_url, 'admin/groups/{}'.format(group['id']))}

Bien cordialement,

Etal Abbot, le gentil robot

-- 
www.data.gouv.fr -- Plateforme ouverte des données publiques françaises
Etalab -- Service du Premier Ministre chargé de l'ouverture des données
               et du développement de la plateforme française Open Data
SGMAP  --  Secrétariat général de la modernisation de l'action publique
État français -- Pyramide administrative fractale et de hauteur infinie
France      --       Un territoire et des citoyens au service de l'État
