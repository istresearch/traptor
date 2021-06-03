import connexion
from traptor import settings

app = connexion.App(__name__, specification_dir=settings.API_DIR)

app.add_api(settings.API_SPEC)

def run_server():
    app.run(port=settings.API_PORT)

if __name__ == '__main__':
    run_server()
