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


% for related_link in dataset['related']:
    % if related_link.get('url') is not None:
<a href="${related_link['url']}">
    % endif
    % if related_link.get('image_url') is not None:
    <img src="${related_link['image_url']}">
    % endif
    % if related_link.get('title') is not None:
    <p><strong>${related_link['title']}</strong></p>
    % endif
    % if related_link.get('description') is not None:
    <div>${related_link['description']}</div>
    % endif
    % if related_link.get('url') is not None:
</a>
    % endif
% endfor
