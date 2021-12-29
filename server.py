from terracotta.server import create_app
import config


def create_tc_server():
    server = create_app()

    return server


app = create_tc_server()

if __name__ == "__main__":
    app.run(port=5000, host="localhost", threaded=False, debug=True)
