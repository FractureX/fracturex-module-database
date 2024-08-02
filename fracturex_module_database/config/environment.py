import os
import json
from dotenv import load_dotenv

from fracturex_module_database.model.database_config import Database_Config

# Cargar variables de entorno
load_dotenv()

class Environment:
    FRACTUREX_MODULE_DATABASE_CONFIG: dict[str, Database_Config] = json.loads(os.getenv("FRACTUREX_MODULE_DATABASE_CONFIG"))

# Valores extraídos del .env
environment: Environment = Environment()
