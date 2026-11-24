"""Réponse commune quand une requête SQL dépasse le `statement_timeout`."""

import logging

from starlette.responses import JSONResponse

logger = logging.getLogger("api_datalake")


def unavailable_response(route: str) -> JSONResponse:
    logger.warning("statement timeout on %s, served 503", route)
    return JSONResponse(
        {"detail": "service temporarily unavailable"},
        status_code=503,
        headers={"Retry-After": "120"},
    )
