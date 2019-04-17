# -*- coding: utf-8 -*-

import os
import setuptools

with open(r'README.md', r'r') as stream:
    long_description = stream.read()

setuptools.setup(
    name=r'hagworm',
    version=r'0.0.22',
    license=r'Apache License Version 2.0',
    platforms=[r'all'],
    author=r'Shaobo.Wang',
    author_email=r'wsb310@gmail.com',
    description=r'Network Development Suite',
    long_description=long_description,
    long_description_content_type=r'text/markdown',
    url=r'https://github.com/wsb310/hagworm',
    packages=setuptools.find_packages(),
    package_data={r'hagworm': [r'static/*.*']},
    install_requires=[
        r'Jinja2==2.10',
        r'aiohttp==3.5.4',
        r'aiomysql==0.0.20',
        r'aioredis==1.2.0',
        r'aiotask_context==0.6.0',
        r'crontab==0.22.5',
        r'cryptography==2.5.0',
        r'flake8==3.7.6',
        r'hiredis==1.0.0',
        r'loguru==0.2.5',
        r'motor==2.0.0',
        r'objgraph==3.4.0',
        r'pillow==5.4.1',
        r'psutil==5.5.1',
        r'PyJWT==1.7.1',
        r'pytest==4.3.0',
        r'pytest-asyncio==0.10.0',
        r'Sphinx==1.8.4',
        r'SQLAlchemy==1.3.1',
        r'tornado==6.0.1',
        r'xlwt==1.3.0',
        r'xmltodict==0.12.0',
        r'yapf==0.26.0',
    ],
    classifiers=[
        r'Programming Language :: Python :: 3.7',
        r'License :: OSI Approved :: Apache Software License',
        r'Operating System :: POSIX :: Linux',
    ],
)
