import connexion
from traptor import settings

app = connexion.App(__name__, specification_dir=settings.API_DIR)

app.add_api(settings.API_SPEC, strict_validation=True, swagger_json=True)

application = app.app
