import setuptools


def long_description():
    with open('README.md', 'r') as file:
        return file.read()


setuptools.setup(
    name='mobius3',
    version='0.0.0',
    author='Department for International Trade',
    author_email='webops@digital.trade.gov.uk',
    description='Continuously and asynchronously sync a local folder to an S3 bucket',
    long_description=long_description(),
    long_description_content_type='text/markdown',
    url='https://github.com/uktrade/mobius3',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
    ],
)
