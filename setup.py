
from setuptools import setup, find_namespace_packages

DEPENDENCIES = [
    "anthill-common>=0.2.5"
]

setup(
    name='anthill-message',
    package_data={
      "anthill.message": ["anthill/message/sql", "anthill/message/static"]
    },
    version='0.2.1',
    description='A messaging service for Anthill platform',
    author='desertkun',
    license='MIT',
    author_email='desertkun@gmail.com',
    url='https://github.com/anthill-platform/anthill-message',
    namespace_packages=["anthill"],
    include_package_data=True,
    packages=find_namespace_packages(include=["anthill.*"]),
    zip_safe=False,
    install_requires=DEPENDENCIES
)
