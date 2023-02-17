import setuptools


def long_description():
    with open('README.md', 'r') as file:
        return file.read()


setuptools.setup(
    name='mobius3',
    version='0.0.40',
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
    python_requires='>=3.7.0',
    py_modules=[
        'mobius3',
    ],
    install_requires=[
        'fifolock>=0.0.20',
        'httpx>=0.23.0',
        'sentry-sdk>=1.11.1'
    ],
    test_suite='test',
    tests_require=[
        'aiohttp==3.*',
        'sentry-sdk>=1.11.1'
    ],
    entry_points={
        'console_scripts': [
            'mobius3=mobius3:main'
        ],
    },
)
