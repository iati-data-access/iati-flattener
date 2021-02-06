from setuptools import setup

requirements = """
lxml==4.6.1
requests==2.24.0
exchangerates==0.3.4
pandas==1.1.4
numpy==1.19.4
PyExcelerate==0.9.0
openpyxl==3.0.5
xlrd==1.2.0
iatikit==2.3.0
"""

setup(
    name="iati-flattener",
    version='0.1.0',
    description="A set of tools to flatten IATI data.",
    author="Mark Brough",
    author_email="mark@brough.io",
    url="https://github.com/iati-data-access/data",
    license="AGPLv3+",
    install_requires=requirements.strip().splitlines(),
    classifiers=(
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        'License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)',
        'Programming Language :: Python :: 3.9'
    )
)