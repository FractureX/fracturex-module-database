from setuptools import setup, find_packages

setup(
    name="fracturex-module-database",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        "pydantic==2.8.2",
        "fastapi==0.111.1",
        "psycopg2==2.9.9",
        "pymongo==4.8.0",
        "python-dotenv==1.0.1"
    ],
    author="FractureX",
    author_email="shaquille.montero.vergel123@example.com",
    description="Módulo de conexión a base de datos",
    url="https://github.com/FractureX/fracturex-module-database"
)