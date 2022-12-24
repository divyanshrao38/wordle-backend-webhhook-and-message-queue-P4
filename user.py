#!/usr/bin/env python3.8
# Imports
import dataclasses
import random
import sqlite3
import textwrap
import databases
import toml
import base64
import hashlib
import secrets
from quart import Quart, g, request, abort, jsonify
from quart_schema import QuartSchema, RequestSchemaValidationError, validate_request, tag

# Encryption type.
ALGORITHM = "pbkdf2_sha256"

# Initialize the app
app = Quart(__name__)
QuartSchema(app, tags=[{"name": "Users", "description": "APIs for creating a user and authenticating a user"} ])
app.config.from_file(f"./etc/wordle.toml", toml.load)


# Decorator to examine class and find fields
@dataclasses.dataclass
class User:
    username: str
    password: str


# Establish database connection
async def _get_db():
    db = getattr(g, "_sqlite_db", None)
    if db is None:
        db = g._sqlite_db = databases.Database(app.config["DATABASES"]["USER_URL"])
        await db.connect()
    return db


# Terminate database connection
@app.teardown_appcontext
async def close_connection(exception):
    db = getattr(g, "_sqlite_db", None)
    if db is not None:
        await db.disconnect()


@tag(["Users"])
@app.route("/register", methods=["POST"])
@validate_request(User)
async def create_user(data):
    """  Create a user """
    db = await _get_db()
    user = dataclasses.asdict(data)
    # Encrypt password
    user["password"] = hash_password(user["password"])
    # Insert into database
    try:
        await db.execute(
            """
            INSERT INTO users(username, password) 
            VALUES (:username, :password)
            """,
            values=user
        )
    # Error
    except sqlite3.IntegrityError as e:
        abort(409, e)
    return {"Message": "User Successfully Created. Please login and create a game"}, 201


@tag(["Users"])
# Endpoint for /login, verifies credentials.
@app.route("/login", methods=["GET"])
async def login():
    """ Authenticate the user """
    db = await _get_db()
    await check_user(db, request.authorization)
    success_response = {"authenticated": True}
    return success_response, 200


async def check_user(db, auth):
    if auth is not None and auth.type == 'basic':
        user_info = await db.fetch_one(
            """
            SELECT password 
            FROM users
            WHERE username = :username
            """,
            values={"username": auth.username}
        )
        if user_info:
            if verify_password(auth.password, user_info["password"]):
                return True
            else:
                abort(401)
        else:
            abort(401)
    else:
        abort(401)


# Hash a given password using pbkdf2.
def hash_password(password, salt=None, iterations=260000):
    if salt is None:
        salt = secrets.token_hex(16)
    assert salt and isinstance(salt, str) and "$" not in salt
    assert isinstance(password, str)
    pw_hash = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), iterations)
    b64_hash = base64.b64encode(pw_hash).decode("ascii").strip()
    return "{}${}${}${}".format(ALGORITHM, iterations, salt, b64_hash)


# Verify a password by comparing it to the hash.
def verify_password(password, password_hash):
    if (password_hash or "").count("$") != 3:
        abort(401)
    algorithm, iterations, salt, b64_hash = password_hash.split("$", 3)
    iterations = int(iterations)
    assert algorithm == ALGORITHM
    compare_hash = hash_password(password, salt, iterations)
    return secrets.compare_digest(password_hash, compare_hash)


# Error status: Client error.
@app.errorhandler(RequestSchemaValidationError)
def bad_request(e):
    return {"error": str(e.validation_error)}, 400


# Error status: Cannot process request.
@app.errorhandler(409)
def conflict(e):
    return {"error": str(e)}, 409


# Error status: Unauthorized client.
@app.errorhandler(401)
def unauthorized(e):
    return {}, 401, {"WWW-Authenticate": "Basic realm='Wordle Site'"}


# Error status: Cannot or will not process the request.
@app.errorhandler(400)
def bad_request(e):
    return jsonify({'message': e.description}), 400
