from setuptools import setup, find_packages


setup(
    name="celery-fallax",
    version="0.0.1",
    author="steph-ben",
    author_email="stephane.benchimol@gmail.com",
    description="Python celery with task factory",
    url="https://github.com/steph-ben/celery-fallax",
    packages=find_packages(include=['celery_fallax', 'celery_fallax.*']),
    entry_points={
        'console_scripts': [
            'fallax-cli=celery_fallax.cli:main',
            'sensors=celery_fallax.sensors.main:main',
        ]
    },
    classifiers=[
        "Programming Language :: Python :: 3",
    ],
    python_requires='>=3.6',
)
