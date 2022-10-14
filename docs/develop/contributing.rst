======================
Contributing
======================

Code Style Check
------------------------
Code style check will be triggered automatically after you submit a PR. So please ensure your PR does not break any of these workflows. It runs ``make format-check``, ``make header-check`` as part of our continuous integration. 
Pull requests should pass ``format-check`` and ``header-check`` without errors before being accepted.

More details can be found at `ci/scripts/run_cpplint.py <https://github.com/intel-innersource/frameworks.ai.modular-sql.velox-plugin/blob/40591b915bfee8068749218725f9c95a4704bacd/ci/scripts/run_cpplint.py>`_

Documentation
------------------------

Cider documentation uses sphinx to produce html structure.
Github pages refer to "docs" directory on "gh-pages" branch.

**Build and commit**

We maintain gh-pages with github actions, which is implemented in .github/workflows/update-gh-pages.yml.

We can simply edit rst files under "docs" directory, when the change merge to "main" branch,
github action will automatically build gh-pages.

If you want to add a new rst file, remember add its title to "index.rst". 

**External links**

Last, share some tools and documents, hope it can help:

- Sphinx quick start: `sphinx-doc <https://www.sphinx-doc.org/en/master/usage/quickstart.html>`_

- How to write rst(reStructuredText) files: `rst-tutorial <https://www.devdungeon.com/content/restructuredtext-rst-tutorial-0>`_

- Transfer markdown to rst: `md-to-rst <https://cloudconvert.com/md-to-rst>`_