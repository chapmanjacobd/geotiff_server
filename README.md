# geotiff_server

```
                _..._
              .'     '.      _
             /    .-""-\   _/ \                       ~+
           .-|   /:.   |  |   |                                *       +
           |  \  |:.   /.-'-./                           '                  |
           | .-'-;:__.'    =/                      ()    .-.,="``"=.    - o -
           .'=  *=|NASA _.='                             '=/_       \     |
          /   _.  |    ;                              *   |  '=._    |
         ;-.-'|    \   |                                   \     `=./ `,        '
        /   | \    _\  _\                               .   '=.__.=' `='      *
        \__/'._;.  ==' ==\                      +                         +
                 \    \   |                       O      *        '       .
                 /    /   /
                 /-._/-._/
          jgs    \   `\  \
                  `-._/._/
```

```sh
mkdir data/
wget -P data/ https://github.com/chapmanjacobd/rasters/raw/main/osm/osm_power_supply.tif
wget -P data/ https://github.com/chapmanjacobd/rasters/raw/main/ookla/ookla_mobile_downloadkbps_2021q1.tif
uv sync
uv run update_db.py
uv run server.py
```
