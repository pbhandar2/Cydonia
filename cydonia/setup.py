from setuptools import setup

setup (
    name="cydonia",
    version="0.1",
    packages=['cydonia.sample', 'cydonia.cachelib', 'cydonia.util'],
    install_requires=["numpy", "pandas", "asserts", "argparse", "mmh3", "boto3", "psutil"]
)