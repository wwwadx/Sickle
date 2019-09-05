# -*- coding: utf-8 -*-
#
# Copyright 2017 Ricequant, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from os.path import dirname, join
try: # for pip >= 10
    from pip._internal.req import parse_requirements
except ImportError: # for pip <= 9.0.3
    from pip.req import parse_requirements


from setuptools import (
    find_packages,
    setup,
)


with open(join(dirname(__file__), 'sickle/VERSION.txt'), 'rb') as f:
    version = f.read().decode('ascii').strip()

setup(
    name='sickle',
    version=version,
    description='sickle for futures multi factors ',
    packages=find_packages(exclude=[]),
    author='wza',
    author_email='wza19930703@gmail.com',
    license='Apache License v2',
    package_data={'': ['*.*']},
    url='https://github.com/wwwadx/Sickle',
    install_requires=[str(ir.req) for ir in parse_requirements(
        "requirements.txt", session=False)],
    zip_safe=False,
    classifiers=[
        'Programming Language :: Python',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: Unix',
        'Programming Language :: Python :: 3.6',
    ],
)