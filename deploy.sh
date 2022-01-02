#!/bin/bash

set -o xtrace         # print commands as they run
set -o errexit        # exit immediately if a pipeline returns a non-zero status
set -o errtrace       # trap ERR from shell functions, command substitutions, and commands from subshell
set -o nounset        # treat unset variables as an error
set -o pipefail       # pipe will exit with last non-zero status if applicable
# set -o noexec       # don't execute; only print
# set -o monitor      # job control is enabled
# set -o noclobber    # prevent output redirection using '>', '>&', '<>' from overwriting existing files


sudo tee /usr/lib/systemd/system/terracotta.service <<EOF
[Unit]
Description=UNLI Terracotta API
After=network.target

[Service]
Restart=always
RestartSec=30
Group=nginx
WorkingDirectory=/home/production/admin/geotiff_server/
Environment="PATH=/home/production/admin/.local/share/virtualenvs/geotiff_server-T2ZenFjo/bin"
ExecStart=/home/production/admin/.local/share/virtualenvs/geotiff_server-T2ZenFjo/bin/gunicorn server:app --workers 2 --bind unix:/run/terracotta.sock -m 007

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now terracotta
systemctl restart terracotta

tee /etc/nginx/sites-available/terracotta <<EOF
server {
    listen 80 default_server;
    listen [::]:80 default_server;
    listen 443 default_server ssl http2;
    listen [::]:443 default_server ssl http2;
    server_name _;

    location /webmap/ {
        proxy_pass http://unix:/run/terracotta.sock:/;
    }
}
EOF

ln -s /etc/nginx/sites-available/terracotta /etc/nginx/sites-enabled/terracotta && rm /etc/nginx/sites-enabled/default && systemctl restart nginx
