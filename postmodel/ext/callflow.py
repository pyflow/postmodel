import logging
from typing import Dict, List, Optional

from callflow import CallFlow  # pylint: disable=E0401

from postmodel import Postmodel
from basepy.asynclog import logger


def register_postmodel(
    app: CallFlow,
    default_db_url,
    extra_db_urls = {},
    modules: Optional[Dict[str, List[str]]] = None,
    generate_schemas: bool = False,
) -> None:
    """
    Registers ``before_server_start`` and ``after_server_stop`` hooks to set-up and tear-down
    Postmodel inside a CallFlow webserver.

    You can configure using ``(db_url, modules)``.

    Parameters
    ----------
    app:
        CallFlow app..
    db_url:
        Use a DB_URL string. See :ref:`db_url`
    modules:
        Dictionary of ``key``: [``list_of_modules``] that defined "apps" and modules that
        should be discovered for models.
    generate_schemas:
        True to generate schema immediately. Only useful for dev environments
    """

    @app.before_start
    async def init_postmodel():  # pylint: disable=W0612
        await Postmodel.init(default_db_url, modules=modules, extra_db_urls=extra_db_urls)
        await logger.info("Postmodel started.")
        if generate_schemas:
            await logger.info("Postmodel generating schema")
            await Postmodel.generate_schemas()

    @app.after_stop
    async def close_postmodel():  # pylint: disable=W0612
        await Postmodel.close()
        await logger.info("Postmodel shutdown.")
