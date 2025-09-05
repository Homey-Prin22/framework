from functools import wraps
from typing import Union
import flask
import requests
from flask_jwt_extended import current_user as current_user_id, get_jwt
from flask_jwt_extended.view_decorators import LocationType
from authentication import verify_token, get_username_from_jwt
from utilities import RedisUtils
from models import User


# Decorators

# Grants access to users with at least one of "roles"
def allow(
        roles: list[str] = [],
        optional: bool = False,
        fresh: bool = False, 
        refresh: bool = False, 
        locations: LocationType = None,
        verify_type: bool = True,
        skip_revocation_check: bool = False
        ):
    def decorator(f):
        @wraps(f)
        @verify_token(optional, fresh, refresh, locations, verify_type, skip_revocation_check)
        def decorator_function(*args, **kwargs):
            # Check authorization only if token is provided
            if get_jwt().get("jti", None) is None:
                return f(*args, **kwargs) 

            # Checking user role
            for user_role in RedisUtils.get_roles(current_user_id):
                if user_role in roles:
                    return f(*args, **kwargs)
            return flask.jsonify(msg = "Forbidden"), 403
        return decorator_function
    return decorator

# Denies access to users with at least one of "roles"
def deny(
        roles: list[str] = [],
        optional: bool = False,
        fresh: bool = False, 
        refresh: bool = False, 
        locations: LocationType = None,
        verify_type: bool = True,
        skip_revocation_check: bool = True  # Recommended method not implemented because it is based on a blocklist
        ):
    def decorator(f):
        @wraps(f)
        @verify_token(optional, fresh, refresh, locations, verify_type, skip_revocation_check)
        def decorator_function(*args, **kwargs):
            # Check authorization only if token is provided
            if get_jwt().get("jti", None) is None:
                return f(*args, **kwargs) 

            # Checking user role
            for user_role in RedisUtils.get_roles(current_user_id):
                if user_role in roles:
                    return flask.jsonify(msg = "Forbidden"), 403
            return f(*args, **kwargs)
        return decorator_function
    return decorator


### ADDED
def check_user_permission() -> Union[flask.Response, None]:
    user_id = current_user_id
    user = User.get_by_id(user_id)
    if not user:
        return flask.jsonify(msg="User not found"), 404
  
    try:
        username = User.get_current_user().username
        response = requests.get(f"http://localhost:5010/api/utils?username={username}")
        if response.status_code != 200:
            return flask.jsonify(msg="Unable to verify data access permissions"), 500

        permissions = response.json()
        # Extract all allowed source_ids from the list of dicts
        allowed_source_ids = [right.get("source_id") for right in permissions if "source_id" in right]

        return allowed_source_ids

    except Exception as e:
        return flask.jsonify(msg="Permission server error", error=str(e)), 500

    # If all checks pass, return None
    return None