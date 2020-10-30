# Release Steps

These are rough and not overly formal steps. Future releases will automate parts and formalize the rest.

## Pre release

1. run `bumpversion --new-version 0.3.0 minor` (change version according to next release version)
1. double check `__init__.py`, `setup.py` and `setup.cfg` to make sure they are labelled correctly
1. Update `HISTORY.rst` with release notes and release date
1. commit changes
1. run `python setup.py sdist bdist_wheel`

## Test release

1. deploy to TestPyPI `python3 -m twine upload --repository testpypi dist/*`
1. go to https://test.pypi.org/project/pynessie/ to make sure everything looks ok
1. download package: `pip install --index-url https://test.pypi.org/simple/ pynessie`, probably in a new venv
1. test package and cli.
1. If anything fails. Fix and update the `setup.py` version to `0.3.0.postN` where `N` starts at 1 and increments for each fix.
   This is because we can't upload the same artifact to test pypi even if there is a problem. This indicates a post release for testpypi
1. re-upload to testpypi and test again
1. once happy remove any `.postN` that may exist in `setup.py` to get back to version as documented in `HISTORY.rst`

## Release

1. deploy to PyPI `python3 -m twine upload dist/*`
1. go to https://pypi.org/project/pynessie/ to make sure everything looks ok
1. download package: `pip install pynessie`, probably in a new venv
1. test package and cli.
1. If anything fails. Fix and use `bumpversion` to either update a patch release (eg `0.3.1` ) or a `post` release (eg `0.3.0.post0`).
1. Update `HISTORY.rst` to account for the new release
1. re-upload to pypi and test again
