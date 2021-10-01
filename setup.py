from setuptools import setup, find_packages

requirements = """
lxml>=4.6.1
requests>=2.24.0
exchangerates>=0.3.4
pandas>=1.1.4
numpy>=1.19.4
PyExcelerate>=0.9.0
openpyxl>=3.0.5
iatikit>=2.3.0
"""

setup(
    name="iatiflattener",
    packages=find_packages(exclude=['ez_setup', 'examples']),
    version='0.9b1',
    description="A set of tools to flatten IATI data.",
    author="Mark Brough",
    author_email="mark@brough.io",
    url="https://github.com/iati-data-access/iati-flattener",
    license="AGPLv3+",
    install_requires=requirements.strip().splitlines(),
    classifiers=(
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        'License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)',
        'Programming Language :: Python :: 3.9'
    ),
    namespace_packages=[],
    include_package_data=True,
    zip_safe=False,
    entry_points={
    }
)