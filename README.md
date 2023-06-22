[![Tests](https://github.com//ckanext-datopian/workflows/Tests/badge.svg?branch=main)](https://github.com//ckanext-datopian/actions)

# CKAN Base Extension for Datopian

This is a CKAN base extension that provides a set of common functionality for Datopian's CKAN-based projects. 

The extension is designed to be highly customizable and modular, allowing you to easily add or remove features as needed. It is intendeted to be fully compatible with the latest version of CKAN.


For more information on how to use the extension, please refer to the documentation included with the extension.


To install ckanext-datopian:

1. Activate your CKAN virtual environment, for example:

     . /usr/lib/ckan/default/bin/activate

2. Clone the source and install it on the virtualenv

    git clone https://github.com//ckanext-datopian.git
    cd ckanext-datopian
    pip install -e .
	pip install -r requirements.txt

3. Add `datopian` to the `ckan.plugins` setting in your CKAN
   config file (by default the config file is located at
   `/etc/ckan/default/ckan.ini`).

4. Restart CKAN. For example if you've deployed CKAN with Apache on Ubuntu:

     sudo service apache2 reload


## Developer installation

To install ckanext-datopian for development, activate your CKAN virtualenv and
do:

    git clone https://github.com//ckanext-datopian.git
    cd ckanext-datopian
    python setup.py develop
    pip install -r dev-requirements.txt
    

## Build new css
It's using scss for styling. To build new css, do:

    yarn  # only once to install dependencies
    gulp bootstrapScss  # only once to copy bootstrap scss files
    gulp build   # to build new css

## Tests

To run the tests, do:

    pytest --ckan-ini=test.ini

