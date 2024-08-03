from setuptools import setup, find_packages

setup(
    name='case',
    version='1.0',
    packages=find_packages(),
    install_requires=[
        'pandas',
        'numpy',
        'pyspark',
    ],
    entry_points={
        'console_scripts': [
            'model_training=case.model_training:main',
            'pre_processing=case.pre_processing:main',
        ],
    },
)