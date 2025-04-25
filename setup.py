from setuptools import setup, find_packages

with open("requirements.txt", encoding="utf-8") as f:
    requirements = f.read().splitlines()

setup(
    name="socceranalytics",
    version='1.0.0',  # quote simple !!!
    packages=find_packages(),
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "socceranalytics = socceranalytics.main:main",
        ],
    },
)
