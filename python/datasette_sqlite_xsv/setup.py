from setuptools import setup

VERSION = "0.1.1-alpha.2"

setup(
    name="datasette-sqlite-xsv",
    description="",
    long_description="",
    long_description_content_type="text/markdown",
    author="Alex Garcia",
    url="https://github.com/asg017/sqlite-xsv",
    project_urls={
        "Issues": "https://github.com/asg017/sqlite-xsv/issues",
        "CI": "https://github.com/asg017/sqlite-xsv/actions",
        "Changelog": "https://github.com/asg017/sqlite-xsv/releases",
    },
    license="MIT License, Apache License, Version 2.0",
    version=VERSION,
    packages=["datasette_sqlite_xsv"],
    entry_points={"datasette": ["sqlite_xsv = datasette_sqlite_xsv"]},
    install_requires=["datasette", "sqlite-xsv"],
    extras_require={"test": ["pytest"]},
    python_requires=">=3.7",
)