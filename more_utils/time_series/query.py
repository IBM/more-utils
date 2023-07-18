"""
Query util to perform all query related operations.
"""

from string import Template
from typing import Dict, Union

SPACE = " "
AND_OPERATOR = "AND"
WHERE = "WHERE"
SELECT = "SELECT * FROM $SCHEMA WHERE TID = $TS_ID"
SELECT_v2 = "SELECT * FROM $MODEL_TABLE"
FROM_TIMESTAMP = "$START_TIME_COLUMN >= '$START_TIME'"
TO_TIMESTAMP = "$END_TIME_COLUMN <= '$END_TIME'"
LIMIT = "LIMIT $LIMIT"


def safe_substitute(query_params: Dict[str, Union[str, int]]):
    """safely substitue query_params into the query string.

    Args:
        query_params (Dict[str, Union[str, int]]): params to
                                                   create a query.

    Returns:
        [str]: A complete query string with placeholder values.

    """
    query = SELECT + SPACE

    if query_params["START_TIME"]:
        query += AND_OPERATOR + SPACE + FROM_TIMESTAMP + SPACE

    if query_params["END_TIME"]:
        query += AND_OPERATOR + SPACE + TO_TIMESTAMP + SPACE

    if query_params["LIMIT"] is None:
        query_params["LIMIT"] = "NULL"

    query += LIMIT

    return Template(query).safe_substitute(query_params)


def safe_substitute_v2(query_params: Dict[str, Union[str, int]]):
    """safely substitue query_params into the query string.

    Args:
        query_params (Dict[str, Union[str, int]]): params to
                                                   create a query.

    Returns:
        [str]: A complete query string with placeholder values.

    """
    query = SELECT_v2 + SPACE

    # if any(key in query_params for key in ["START_TIME","END_TIME"]):
    #     query += WHERE + SPACE + AND_OPERATOR + SPACE

    if query_params["START_TIME"]:
        query += WHERE + SPACE + FROM_TIMESTAMP + SPACE

    if query_params["END_TIME"]:
        if query_params["START_TIME"]:
            query += AND_OPERATOR + SPACE
        else:
            query += WHERE + SPACE
        query += TO_TIMESTAMP + SPACE

    if query_params["LIMIT"] is None:
        query_params["LIMIT"] = "NULL"

    query += LIMIT

    return Template(query).safe_substitute(query_params)
