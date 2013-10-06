## -*- coding: utf-8 -*-


<%!
import urlparse
%>


From: ${from_email}
To: ${u', '.join(to_emails)}
Subject: ${u"[data] Nouveau jeu de données: {}".format(dataset['title']) | qp}
MIME-Version: 1.0
Content-Type: text/plain; charset="${encoding}"

Salut,

Juste un petit mot pour te signaler qu'un nouveau jeu de données vient d'être créé :
  ${dataset['title']}

Pour le regarder :
  ${urlparse.urljoin(weckan_url, 'dataset/{}'.format(dataset['name']))}

Si tu n'es pas débordé par tes rendez-vous, tes réunions, tes rapports, tes courriels, tes coups de fils, la réorganisation en cours, le prochain déménagement, etc, tu peux aussi faire ton boulot et surveiller son contenu :
  ${urlparse.urljoin(ckan_of_worms_url, 'admin/datasets/{}'.format(dataset['id']))}

Bien cordialement,

Etal Abbot, le gentil robot

-- 
www.data.gouv.fr -- Plateforme ouverte des données publiques françaises
Etalab -- Service du Premier Ministre chargé de l'ouverture des données
               et du développement de la plateforme française Open Data
SGMAP  --  Secrétariat général de la modernisation de l'action publique
État    --    Pyramide bureaucratique infinie déconnectée de la réalité
France      --       Un territoire et des citoyens au service de l'État
