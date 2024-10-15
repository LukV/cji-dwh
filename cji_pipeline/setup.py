from setuptools import find_packages, setup

setup(
    name="cji_pipeline",
    packages=find_packages(exclude=["cji_pipeline_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
