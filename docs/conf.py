"""Sphinx configuration for the AGCoord documentation site."""

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as distribution_version

project = "AGCoord"
author = "AGCoord contributors"
copyright = "2026, AGCoord contributors"

# Read the Docs installs the package before building, so the site reports the version of
# the distribution it documents instead of carrying another hand-maintained copy.
try:
    release = distribution_version("agcoord")
except PackageNotFoundError:  # a docs build without the package installed
    release = "0.0.0"
version = release

extensions = [
    "myst_parser",
]

myst_enable_extensions = [
    "colon_fence",
    "deflist",
    "fieldlist",
]
# The coordinator guide links to a level-4 heading (the pytest-xdist adapter section).
myst_heading_anchors = 4

templates_path = []
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store", "requirements.txt"]

html_theme = "furo"
html_title = "AGCoord"
html_logo = "assets/agcoord-gourd-mascot.png"
html_static_path = []

# GitHub-flavored source links in the theme footer
html_theme_options = {
    "source_repository": "https://github.com/xu-hao/agcoord",
    "source_branch": "main",
    "source_directory": "docs/",
}
