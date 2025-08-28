from setuptools import find_packages, setup

setup(
    name="zyte_spidermon",
    version="0.0.1",
    url="https://github.com/scrapy-plugins/zyte-spidermon",
    author="Zyte",
    author_email="opensource@zyte.com",
    description=("Zyte spidermon is a collection of useful tools for data validation, stats monitoring, and notification messages that can be use within Zyte")
    packages=["zyte_spidermon"],
    install_requires=[
        "spidermon",
    ],
)
