import config
from terracotta import get_settings
from terracotta.server import create_app


def create_tc_server():
    return create_app(debug=get_settings().DEBUG)


app = create_tc_server()

if __name__ == "__main__":
    # Never enable the Werkzeug debugger implicitly: it allows arbitrary code
    # execution. Enable it explicitly with TC_DEBUG=true for local development.
    app.run(
        port=5000,
        host="127.0.0.1",
        threaded=False,
        debug=get_settings().DEBUG,
    )
