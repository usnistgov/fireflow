# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "pyreflow"
copyright = "2025, Nate Dwarshuis"
author = "Nate Dwarshuis"
release = "0.1.0"


# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.intersphinx",
    "sphinx.ext.autodoc",
    "sphinx.ext.graphviz",
    "sphinx.ext.githubpages",
    "sphinx_toolbox.more_autodoc.genericalias",
]

templates_path = ["_templates"]
exclude_patterns = []

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "alabaster"
html_sidebars = {
    "**": [
        "about.html",
        "searchbox.html",
        "globaltoc.html",
    ]
}
html_theme_options = {
    "github_button": "true",
    "github_user": "usnistgov",
    "github_repo": "fireflow",
}
# html_static_path = ["_static"]
html_show_sourcelink = False

autodoc_typehints = "description"
autodoc_member_order = "bysource"

# TODO add custom inventory for polars using https://sphobjinv.readthedocs.io/en/latest/
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "numpy": ("https://numpy.org/doc/stable/", None),
}


def process_bases(app, name, obj, options, bases):
    # import lazily to avoid cycles
    import importlib

    pft = getattr(importlib.import_module("pyreflow"), "typing")
    this_name = getattr(obj, "__name__")
    extra = next(
        (v for k, v in pft._ABC_MAP.items() if k.__name__ == this_name),
        [],
    )

    for cls in extra:
        bases.append(cls)


def setup(app):
    app.connect("autodoc-process-bases", process_bases)


# include some type aliases

autodoc_type_aliases = {
    "Selector": "pyreflow.typing.Selector",
    "Condition": "pyreflow.typing.Condition",
    "Statement": "pyreflow.typing.Statement",
}
