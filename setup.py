import setuptools


def long_description():
    with open('README.md', 'r') as file:
        return file.read()


setuptools.setup(
    name='mobius3',
    version='0.0.34',
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
        'lowhaio>=0.0.85',
        'lowhaio-aws-sigv4-unsigned-payload>=0.0.4',
        'lowhaio-retry>=0.0.5'
    ],
    test_suite='test',
    tests_require=[
        'aiohttp~=3.5.4',
        'lowhaio-aws-sigv4>=0.0.4',
    ],
    entry_points={
        'console_scripts': [
            'mobius3=mobius3:main'
        ],
    },
)
