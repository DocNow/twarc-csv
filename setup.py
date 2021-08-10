import setuptools

with open("README.md") as f:
    long_description = f.read()

setuptools.setup(
    name="twarc-csv",
    version="0.3.8",
    url="https://github.com/docnow/twarc-csv",
    author="Igor Brigadir",
    author_email="igor.brigadir@gmail.com",
    py_modules=["twarc_csv"],
    description="A twarc plugin to output Twitter data as CSV",
    long_description=long_description,
    long_description_content_type="text/markdown",
    python_requires=">=3.3",
    install_requires=[
        "twarc>=2.4.0",
        "pandas>=1.2.5",
        "more-itertools>=8.7.0",
        "tqdm>=4.59.0",
    ],
    setup_requires=["pytest-runner"],
    tests_require=["pytest"],
    entry_points="""
        [twarc.plugins]
        csv=twarc_csv:csv
    """,
)
