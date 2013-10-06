## -*- coding: utf-8 -*-


<%!
import urlparse
%>


From: ${from_email}
To: ${u', '.join(to_emails)}
Subject: ${u"[data] Nouveau compte : {}".format(account['name']) | qp}
MIME-Version: 1.0
Content-Type: text/plain; charset="${encoding}"

Salut,

Juste un petit mot pour te signaler qu'un nouveau compte utilisateur vient d'être créé :
* Nom complet : ${account.get('fullname') or u''}
* Identifiant de connexion : ${account.get('name') or u''}
* Courriel : ${account.get('email') or u''}

Pour le regarder :
  ${urlparse.urljoin(weckan_url, 'user/{}'.format(account['name']))}

Si tu n'es pas débordé par tes rendez-vous, tes réunions, tes formations, tes rapports, tes courriels, tes coups de fils, la réorganisation en cours, le prochain déménagement, etc, tu peux aussi faire ton boulot et surveiller son compte :
  ${urlparse.urljoin(ckan_of_worms_url, 'admin/accounts/{}'.format(account['id']))}

Bien cordialement,

Etal Abbot, le gentil robot

-- 
www.data.gouv.fr -- Plateforme ouverte des données publiques françaises
Etalab -- Service du Premier Ministre chargé de l'ouverture des données
               et du développement de la plateforme française Open Data
SGMAP  --  Secrétariat général de la modernisation de l'action publique
État français -- Pyramide administrative fractale et de hauteur infinie
France      --       Un territoire et des citoyens au service de l'État
