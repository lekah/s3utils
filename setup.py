from setuptools import setup, find_packages

setup(
    name="s3utils",
    version="0.0.1",
    description="",
    download_url="",
    author="Leonid Kahle",
    python_requires=">=3.6",
    packages=find_packages(),
    zip_safe=True,
    install_requires=[
        'boto3',
    ]
)
