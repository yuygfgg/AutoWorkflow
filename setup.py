from setuptools import setup, find_packages
import pathlib


BASE_DIR = pathlib.Path(__file__).parent.resolve()
README = (
    (BASE_DIR / "README.md").read_text(encoding="utf-8")
    if (BASE_DIR / "README.md").exists()
    else ""
)


setup(
    name="autoworkflow",
    version="0.0.1",
    description="Declarative, type-safe automation/workflow engine for Python",
    long_description=README,
    long_description_content_type="text/markdown",
    author="yuygfgg",
    author_email="me@yuygfgg.com",
    url="https://github.com/yuygfgg/autoworkflow",
    packages=find_packages(exclude=("tests*", "examples*")),
    include_package_data=True,
    python_requires=">=3.10",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries",
        "Intended Audience :: Developers",
    ],
    license="GPL-3.0-or-later",
)
