# Imports
import dataclasses
import itertools
import random
import textwrap
import uuid
import databases
import toml
from quart import Quart, g, request, abort, jsonify
from quart_schema import QuartSchema, RequestSchemaValidationError, validate_request, tag
from redis import Redis
import rq
import httpx
import time

# Initialize the app
app = Quart(__name__)
QuartSchema(app, tags=[
                       {"name": "Games", "description": "APIs for creating and playing a game for a particular user"},
                       {"name": "Statistics", "description": "APIs for checking game statistics for a user"},
                       {"name": "Root", "description": "Root path returning html"}
                    ])
app.config.from_file(f"./etc/wordle.toml", toml.load)


@dataclasses.dataclass
class Word:
    guess: str


db_list = ['PRIMARY_GAME_URL', 'SECONDARY_GAME_URL', 'TERTIARY_GAME_URL']
iterator = itertools.cycle(db_list)


# Establish database connection
async def _get_read_db(db):
    db = g._sqlite_read_db = databases.Database(app.config["DATABASES"][db])
    await db.connect()
    return db


async def _get_write_db():
    db = g._sqlite_write_db = databases.Database(app.config["DATABASES"]['PRIMARY_GAME_URL'])
    await db.connect()
    return db


# Terminate database connection
@app.teardown_appcontext
async def close_connection(exception):
    write_db = getattr(g, "_sqlite_write_db", None)
    if write_db is not None:
        await write_db.disconnect()
    read_db = getattr(g, "_sqlite_read_db", None)
    if read_db is not None:
        await read_db.disconnect()


@tag(["Root"])
@app.route("/", methods=["GET"])
async def index():
    """ Root path, returns HTML """
    return textwrap.dedent(
        """
        <h1>Wordle Game</h1>
        <p>To play wordle, go to the <a href="http://tuffix-vm/docs">Games Docs</a></p>\n
        """
    )


@tag(["Games"])
@app.route("/games", methods=["POST"])
async def create_game():
    """ Create a game """
    iter_val = next(iterator)
    read_db = await _get_read_db(iter_val)
    write_db = await _get_write_db()
    username = request.authorization.username

    # Open a file and load json from it
    res = await read_db.fetch_one(
        """
        SELECT count(*) count FROM correct_words
        """
    )
    length = res.count
    uuid1 = str(uuid.uuid4())

    await write_db.execute(
        """
        INSERT INTO games(game_id, username, secret_word_id)
        VALUES(:uuid, :user, :secret_word_id)
        """,
        values={"uuid": uuid1, "user": username, "secret_word_id": random.randint(1, length)}
    )

    return {"game_id": uuid1, "message": "Game Successfully Created"}, 200


@validate_request(Word)
@tag(["Games"])
@app.route("/games/<string:game_id>", methods=["POST"])
async def play_game(game_id):
    """ Play the game (creating a guess) """
    data = await request.json
    read_db = await _get_read_db(next(iterator))
    write_db = await _get_write_db()

    username = request.authorization.username

    return await play_game_or_check_progress(read_db, write_db, username, game_id, data["guess"])


@tag(["Games"])
@app.route("/games/<string:game_id>", methods=["GET"])
async def check_game_progress(game_id):
    """ Check the state of a game that is in progress. If game is over show whether user won/lost and no. of guesses """
    read_db = await _get_read_db(next(iterator))
    write_db = await _get_write_db()
    username = request.authorization.username

    return await play_game_or_check_progress(read_db, write_db, username, game_id)


@tag(["Statistics"])
@app.route("/games", methods=["GET"])
async def get_in_progress_games():
    """ Check the list of in-progress games for a particular user """
    read_db = await _get_read_db(next(iterator))
    username = request.authorization.username

    # showing only in-progress games
    games_output = await read_db.fetch_all(
        """
        SELECT guess_remaining, game_id, state
        FROM games
        WHERE username =:username AND state = :state
        """,
        values={"username": username, "state": 0}
    )

    in_progress_games = []
    for guess_remaining, game_id, state in games_output:
        in_progress_games.append({
            "guess_remaining": guess_remaining,
            "game_id": game_id
        })

    return in_progress_games


@tag(["Statistics"])
@app.route("/games/statistics", methods=["GET"])
async def statistics():
    """ Checking the statistics for a particular user """
    db = await _get_read_db(next(iterator))
    username = request.authorization.username

    res_games = await db.fetch_all(
        """
        SELECT state, count(*)
        FROM games
        WHERE username=:username
        GROUP BY state
        """,
        values={"username": username}
    )
    states = {0: 'In Progress', 1: 'win', 2: "loss"}
    games_stats = {}
    for state, count in res_games:
        games_stats[states[state]] = count

    return games_stats

@tag(["ClientRegister"])
@app.route("/client_register", methods=["POST"])
async def client_register():
    # Allowing client registration
    app.logger.info("Client is registering a new url")
    data = await request.form
    # Get the call back url from client
    callback_url = data.get("url")
    # Get readable database
    read_db = await _get_read_db(next(iterator))
    # Get writable database
    write_db = await _get_write_db()
    # Store the url in database
    return await save_callbakc_urls(read_db, write_db, callback_url)


async def play_game_or_check_progress(read_db, write_db, username, game_id, guess=None):
    states = {0: 'In Progress', 1: 'win', 2: "loss"}
    games_output = await read_db.fetch_one(
        """
        SELECT correct_words.correct_word secret_word, guess_remaining, state
        FROM games join correct_words WHERE username=:username
        AND game_id=:game_id AND correct_words.correct_word_id=games.secret_word_id
        """,
        values={"game_id": game_id, "username": username}
    )

    if not games_output:
        abort(400, "No game with this identifier for your username")

    if games_output["state"] != 0:
        return {"number_of_guesses": 6 - games_output["guess_remaining"],
                "decision": states.get(games_output["state"])}, 200

    secret_word = games_output["secret_word"]
    state = 0
    guess_remaining = games_output["guess_remaining"]

    if guess is None:
        guess_output = await fetch_guesses(read_db, game_id)
    else:
        if len(guess) != 5:
            abort(400, "Bad Request: Word length should be 5")
        # when the user guessed the correct word
        if guess == secret_word:
            state = 1

        valid_word_output = await read_db.fetch_one(
            """
            SELECT valid_word_id
            FROM valid_words
            WHERE valid_word =:word
            """,
            values={"word": guess}
        )
        if not valid_word_output:
            if not state:
                abort(400, "Bad Request: Not a valid guess")

        # Decrement the guess remaining
        guess_remaining = guess_remaining - 1
        guess_number = 6 - guess_remaining
        # Game is over, update the game
        if guess_remaining == 0 or state:
            # user lost the game
            if guess_remaining == 0 and state == 0:
                state = 2
            await write_db.execute(
                """
                UPDATE games
                SET guess_remaining=:guess_remaining, state=:state
                WHERE game_id=:game_id
                """,
                values={"guess_remaining": guess_remaining, "game_id": game_id, "state": state}
            )
            game_data = {"status": states[state], "username": username, "guess_number": guess_number}
            await enqueue_game_status(read_db, game_data)

            return {"game_id": game_id, "number_of_guesses": 6 - guess_remaining, "decision": states[state]}, 200

        # else prepare the response and insert into guesses afterwards to ensure read-your-write consistency

        valid_word_id = valid_word_output.valid_word_id

        guess_output = await fetch_guesses(read_db, game_id)

        new_guess = (guess_number, guess)
        guess_output.append(new_guess)

        await write_db.execute(
            """
            UPDATE games
            SET guess_remaining=:guess_remaining
            WHERE game_id=:game_id
            """,
            values={"guess_remaining": guess_remaining, "game_id": game_id}
        )

        await write_db.execute(
            """
            INSERT INTO guesses(game_id, valid_word_id, guess_number)
            VALUES(:game_id, :valid_word_id, :guess_number)
            """,
            values={"game_id": game_id, "valid_word_id": valid_word_id, "guess_number": guess_number}
        )

    guesses = []
    for guess_number, valid_word in guess_output:
        correct_positions, incorrect_positions = compare(secret_word, valid_word)
        guesses.append(
            {
                "guess": valid_word,
                "guess_number": guess_number,
                "correct_positions": dict(correct_positions),
                "incorrect_positions:": dict(incorrect_positions),
            }
        )

    return {"guesses": guesses, "guess_remaining": guess_remaining, "game_state": states[state]}, 200


def send_scores_job(url, game_results):

    response = httpx.post(url, json=game_results)
    return response


async def enqueue_game_status(read_db, game_results):
    callback_url_output = await read_db.fetch_all("SELECT url from callback_urls")
    # for each url enqueue a job to send the scores
    if len(callback_url_output) < 0:
        return
    callback_url = callback_url_output[0]
    app.logger.info(callback_url)
    for url in callback_url:

        queue = rq.Queue(connection=Redis())
        job = queue.enqueue(send_scores_job, url, game_results)

        if job is not None:
            time.sleep(2)
            app.logger.info(job.result)


async def fetch_guesses(read_db, game_id):
    # Prepare the response
    guess_output = await read_db.fetch_all(
        """
        SELECT guess_number, valid_words.valid_word
        FROM guesses
        JOIN valid_words
        WHERE game_id=:game_id AND valid_words.valid_word_id=guesses.valid_word_id
        ORDER BY guess_number
        """,
        values={"game_id": game_id}
    )

    return guess_output

# Function to compare the guess to answer.
def compare(secret_word, guess):
    secret_word_lst = [i for i in enumerate(secret_word)]
    guess_list = [i for i in enumerate(guess)]

    temp_correct_positions = []
    correct_positions = []
    incorrect_positions = []
    for i in range(0, len(secret_word)):
        if guess_list[i][1] == secret_word_lst[i][1]:
            temp_correct_positions.append(guess_list[i])
            correct_positions.append(((guess_list[i][0] + 1), guess_list[i][1]))

    secret_word_lst = [i for i in secret_word_lst if i not in temp_correct_positions]
    guess_list = [i for i in guess_list if i not in temp_correct_positions]

    for i in range(len(guess_list)):
        for j in range(len(secret_word_lst)):
            # found a character which is in a different position
            if guess_list[i][1] == secret_word_lst[j][1]:
                incorrect_positions.append((guess_list[i][0] + 1, guess_list[i][1]))
                secret_word_lst.pop(j)
                break

    return correct_positions, incorrect_positions


async def save_callbakc_urls(read_db, write_db, url):
    # Before saving the url, check if the url is already existed
    url_result = await read_db.fetch_one("select * from callback_urls where url=:url", {"url":url})

    if url_result:
        return {"message" : "The url is already in database, so skip saving."}, 200

    guess_output = await write_db.execute(
        """
        INSERT INTO callback_urls(url)
        VALUES(:url)
        """,
        values={"url": url}
    )
    return {"message" : "saved the callback url to database."}, 200

# Error status: Client error.
@app.errorhandler(RequestSchemaValidationError)
def bad_request(e):
    return {"error": str(e.validation_error)}, 400


# Error status: Cannot or will not process the request.
@app.errorhandler(400)
def bad_request(e):
    return jsonify({'message': e.description}), 400
