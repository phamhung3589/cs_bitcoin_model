from setuptools import setup, find_packages
setup(
    name='cs_bitcoin_model',
    version='0.0.5',
    keywords=("cs_bitcoin_model", "cs", "bitcoin model"),
    url="https://github.com/phamhung3589/cs_bitcoin_model.git",
    author='hungph',
    author_email='phamhung3589@gmail.com',
    description='predict bitcoin based on data everyday',
    packages=find_packages(),
    license="MIT Licence",
    platforms="any",
    package_data={'': ['nginx.cryptosloth.io.crt', 'nginx.cryptosloth.io.key', 'production.config', 'test.config', 'Binance_BTCUSDT_1h.csv']},
    include_package_data=True,
    install_requires=['numpy', 'uvicorn', 'binance.py', 'pandas', 'requests', 'fastapi', 'pydantic'],
    python_requires='>=3.6',
)