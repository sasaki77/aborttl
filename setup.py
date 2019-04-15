from setuptools import find_packages, setup

with open('README.md') as f:
        readme = f.read()

with open('LICENSE') as f:
        license = f.read()

setup(
    name='aborttl',
    version='0.0.1',
    url='https://github.com/sasaki77/aborttl',
    license=license,
    maintainer='Shinya Sasaki',
    maintainer_email='shinya.sasaki@kek.jp',
    description='Abort Time Logger',
    long_description=readme,
    packages=find_packages('src'),
    package_dir={'': 'src'},
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'SQLAlchemy',
        'pyepics',
    ],
    extras_require={
        'develop': [
            'pytest',
            'pytest-cov',
            'pycodestyle'
            ]
    },
)
