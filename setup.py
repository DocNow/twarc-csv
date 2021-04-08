import setuptools

with open("README.md") as f:
    long_description = f.read()

setuptools.setup(
    name="twarc-csv",
    version="0.0.1",
    url="https://github.com/docnow/twarc-csv",
    author="Igor Brigadir",
    author_email="igor.brigadir@gmail.com",
    py_modules=["twarc_csv"],
    description="A twarc plugin to output Twitter data as CSV",
    long_description=long_description,
    long_description_content_type="text/markdown",
    python_requires=">=3.3",
    install_requires=["twarc>=2.0.4", "pandas"],
    setup_requires=["pytest-runner"],
    tests_require=["pytest"],
    entry_points="""
        [twarc.plugins]
        csv=twarc_csv:csv
    """,
)
