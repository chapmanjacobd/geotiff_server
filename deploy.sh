#!/bin/bash

set -o xtrace         # print commands as they run
set -o errexit        # exit immediately if a pipeline returns a non-zero status
set -o errtrace       # trap ERR from shell functions, command substitutions, and commands from subshell
set -o nounset        # treat unset variables as an error
set -o pipefail       # pipe will exit with last non-zero status if applicable
# set -o noexec       # don't execute; only print
# set -o monitor      # job control is enabled
# set -o noclobber    # prevent output redirection using '>', '>&', '<>' from overwriting existing files


tee /etc/systemd/system/terracotta.service <<EOF
[Unit]
Description=Gunicorn Terracotta
After=network.target

[Service]
Restart=always
RestartSec=30
User=www-data
Group=www-data
WorkingDirectory=/root
ExecStart=/usr/local/bin/gunicorn server:app --workers 2 --bind unix:root/terracotta.sock -m 007

[Install]
WantedBy=multi-user.target
EOF

systemctl enable --now terracotta

tee /etc/nginx/sites-available/terracotta <<EOF
server {
    listen 80 default_server;
    listen [::]:80 default_server;
    listen 443 default_server ssl http2;
    listen [::]:443 default_server ssl http2;
    server_name _;

    location /api/ {
        include proxy_params;
        proxy_pass http://unix:/root/terracotta.sock:/;
    }
}
EOF

ln -s /etc/nginx/sites-available/terracotta /etc/nginx/sites-enabled/terracotta && rm /etc/nginx/sites-enabled/default && systemctl restart nginx

pip3 install Cython numpy gunicorn httplib2 pandas scipy rasterio rio-cogeo crick netCDF4 Flask dash pyproj dash-leaflet git+https://github.com/chapmanjacobd/terracotta.git@9bd58730f551258353c81c6e792b010c008cfc23#egg=terracotta
