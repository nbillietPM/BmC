from setuptools import setup, find_packages

setup(
    name="BmC",  # Replace with your actual project name
    version="0.1",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    install_requires=[],
    python_requires=">=3.8",
    entry_points={
        "console_scripts": [
            "gbif-query = datasource.gbif:main",  # CLI command: `gbif-query`
        ]
    }
)