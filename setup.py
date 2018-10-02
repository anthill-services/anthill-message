
from setuptools import setup, find_packages

DEPENDENCIES = [
    "anthill-common"
]

setup(
    name='anthill-message',
    setup_requires=["pypigit-version"],
    git_version="0.1.0",
    description='A messaging service for Anthill platform',
    author='desertkun',
    license='MIT',
    author_email='desertkun@gmail.com',
    url='https://github.com/anthill-platform/anthill-message',
    namespace_packages=["anthill"],
    packages=find_packages(),
    zip_safe=False,
    install_requires=DEPENDENCIES
)
