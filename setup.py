from setuptools import setup, find_packages
setup(
    name='cs_bitcoin_model',
    version='0.0.1',
    url="https://github.com/phamhung3589/cs_bitcoin_model.git",
    author='hungph',
    author_email='phamhung3589@gmail.com',
    description='predict bitcoin based on data everyday',
    packages=find_packages(),
    classifiers=[
    'Programming Language :: Python :: 3',
    'License :: OSI Approved :: MIT License',
    'Operating System :: OS Independent',
    ],
    platforms="any",
    install_requires=['numpy', 'uvicorn', 'binance.py', 'pandas', 'requests', 'fastapi', 'pydantic'],
    python_requires='>=3.6',
)