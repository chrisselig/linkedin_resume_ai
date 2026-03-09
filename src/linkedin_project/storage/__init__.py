"""Storage sub-package — public API.

.. autofunction:: linkedin_project.storage.database.connect
.. autofunction:: linkedin_project.storage.database.create_tables
.. autofunction:: linkedin_project.storage.database.upsert_profile
.. autofunction:: linkedin_project.storage.database.query_section
"""

from linkedin_project.storage.database import (
    connect,
    create_tables,
    query_section,
    upsert_profile,
)

__all__ = [
    "connect",
    "create_tables",
    "upsert_profile",
    "query_section",
]
