from setuptools import find_packages, setup

setup(
    name="spotify_ads",
    packages=find_packages(exclude=["quickstart_gcp_tests", "quickstart_gcp"]),
    package_data={"spotify_ads": ["spotify_ads_dbt/*"]},
    install_requires=[
        "dagster",
        "dagster-gcp",
        "dagster-gcp-pandas",
        "dagster-cloud",
        "dagster-dbt",
        "dbt-bigquery",
        "dbt-core",
        "boto3",  # used by Dagster Cloud Serverless
        "pandas",       
        "pandas_gbq",
        "google-auth",
    ],
    extras_require={"dev": ["dagster-webserver==1.5.13", "pytest"]},
)
