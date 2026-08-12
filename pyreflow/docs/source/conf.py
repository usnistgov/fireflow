from docutils import nodes  # type: ignore

#
# Project information
#
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "pyreflow"
copyright = "2025, Nate Dwarshuis"
author = "Nate Dwarshuis"
release = "0.1.0"

#
# General configuration
#
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

#
# Options for HTML output
#
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

#
# Cross References
#
# For the most part this just works given autodoc and
# sphinx_toolbox.more_autodoc.genericalias. However, sphinx occasionally will
# screw up cross references because either it can't find something in
# objects.inv or it tries to put a reference where there shouldn't be one.
#
# ASSUMES that sphinx is run with nitpicky which will complain loudly if any
# references are broken (which is what I want).

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "numpy": ("https://numpy.org/doc/stable/", None),
    "polars": ("https://docs.pola.rs/api/python/stable/", None),
}

# x-refs that won't link properly for whatever reason
_XREF_FALSE_NEGATIVES = {
    # polars for whatever reason doesn't emit most of its classes in the
    # objects.inv file
    ("py:class", "polars.DataFrame"): (
        "https://docs.pola.rs/api/python/stable/reference/dataframe/index.html",
        "polars.DataFrame",
    ),
    # ditto
    ("py:class", "polars.Series"): (
        "https://docs.pola.rs/api/python/stable/reference/series/index.html",
        "polars.Series",
    ),
}

# x-refs that sphinx mistakenly thinks exist but really shouldn't
_XREF_FALSE_POSITIVES = {
    # this is a generic type variable that sphinx will try to document if it
    # appears by itself (ie not encosed in "[]" as a parameter for another
    # generic type
    "T"
}


def resolve_missing_xref(app, env, node, contnode):
    """Fix broken cross-references.

    These may either be false positives (sphinx thinks a link should exist where
    there is no link) or false negatives (sphinx doesn't have a link so it
    doesn't insert one). Deal with both cases here
    """

    # First, see if this node should be linked at all. If it is in the negative
    # filter, return node as-is with no link.
    target = node["reftarget"]
    if node["reftype"] == "class" and target in _XREF_FALSE_POSITIVES:
        return contnode

    # Otherwise, look up what the target should be and return a reference node
    key = (f"{node['refdomain']}:{node['reftype']}", target)
    hit = _XREF_FALSE_NEGATIVES.get(key)
    if hit is None:
        return None
    uri, label = hit
    ref = nodes.reference("", "", internal=False, refuri=uri, reftitle=label)
    ref.append(contnode)
    return ref


def process_bases(app, name, obj, options, bases):
    """Fix class inheritance "Bases: " line.

    Specifically show ABC class "inheritance" (which isn't real inheritance but
    I want to document as such) and don't show "object" where it is redundant.
    """
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
    if len(bases) > 1:
        bases[:] = [b for b in bases if b.__name__ != "object"]


def setup(app):
    app.connect("missing-reference", resolve_missing_xref)
    app.connect("autodoc-process-bases", process_bases)
