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

## Configuration and security

Terracotta reads `TC_*` environment variables, but settings in `config.py` take
precedence. CORS origins are JSON lists and can be restricted without editing
the file:

```sh
TC_ALLOWED_ORIGINS_TILES='["https://example.com"]' uv run server.py
```

Debug mode is off by default because the Werkzeug debugger allows arbitrary
code execution. Only enable it for local development:

```sh
TC_DEBUG=true uv run server.py
```

Run `uv audit` after changing dependencies to check for known vulnerabilities.

