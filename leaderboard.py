# Imports
import dataclasses
import os
import socket
import time

from quart import Quart, jsonify, abort
from quart_schema import QuartSchema, tag, validate_request, RequestSchemaValidationError
import redis
import httpx

# Initialize the app
app = Quart(__name__)
QuartSchema(app, tags=[
    {"name": "Leaderboard", "description": "APIs for posting the results of the leaderboard service"}])

#*** To register with the Games service at startup by making an HTTP request to the Games service using the HTTPX client library ***
# Construct the callback url

def client_register_url(url):
    try:
        app.logger.info("The client is registering url.")
        result = httpx.post("http://tuffix-vm/client_register", data={"url": url})
        if result.status_code != 200:
        	result = client_register_url(callback_url)
        return result
    except httpx.HTTPError as e:
        app.logger.error("Register call back url failed, retry now in 1 second.")
        # wait one second then resend the request
        time.sleep(1)
        return client_register_url(url)


# Get port number from system environment, if fail to get, then set the default port to 5400
port = os.environ.get("PORT", "5400")
# Form the callback url, this will form an address like: http://127.0.0.1:5400/
# If you replace "localhost" with nothing or "tuffix-vm", you will get 127.0.1.1
callback_url = "http://" + socket.gethostbyname(socket.getfqdn("localhost")) + ":" + port + "/results"
response = client_register_url(callback_url)



@dataclasses.dataclass
class Result:
    username: str
    status: str
    guess_number: int


def _initialize_redis():
    r = redis.Redis()
    return r


@tag(["Leaderboard"])
@app.route("/results", methods=["POST"])
@validate_request(Result)
async def add_game_results(data):
    app.logger.info("----------")
    app.logger.info(data)
    """ Posting the results of the game. Pass username, status as win/loss and the number of guesses"""
    data = dataclasses.asdict(data)
    r = _initialize_redis()
    status = data['status']
    username = data['username']

    if status not in ['win', 'loss']:
        abort(400, "Please pass the status of the game as either win or loss")
    guess_number = data['guess_number']
    if status == 'loss' and guess_number != 6:
        abort(400, "Loss always requires 6 guesses")
    # compute score from the number of guess and game status
    if status == 'loss':
        game_score = 0
    else:

        if guess_number < 1 or guess_number > 6:
            abort(400, "Please enter the guess number between 1 and 6 if game status is win")

        game_score = 6 - guess_number + 1

    # if redis key is not created it automatically create a key

    # increment total of the game score by the current game score
    r.hincrby("users:" + username, "total_score", game_score)
    total_score = int(r.hget("users:" + username, "total_score").decode("UTF-8"))

    # increment the number of games by 1
    r.hincrby("users:" + username, "game_count", 1)
    number_of_games = int(r.hget("users:" + username, "game_count").decode("UTF-8"))

    avg_score = total_score / number_of_games

    r.zadd("wordle_leaderboard", {"users:" + username: avg_score})
    return {"Message": "Game results successfully posted."}, 201


@tag(["Leaderboard"])
@app.route("/leaderboard", methods=["GET"])
async def leaderboard():
    """ Retrieve the list of top 10 users of the wordle game based on their average scores """
    r = _initialize_redis()

    avg_score_result = r.zrevrange("wordle_leaderboard", 0, 9, True)
    # prepare response

    # no users in the redis
    if len(avg_score_result) == 0:
        return "Please post results to retrieve the top 10 users by average score", 200

    leaderboard_result = []

    for user, score in avg_score_result:
        user = user.decode('UTF-8').split(':')[1]
        leaderboard_result.append({"username": user,  "score": score})

    return leaderboard_result, 200


# Error status: Cannot or will not process the request.
@app.errorhandler(400)
def bad_request(e):
    return jsonify({'message': e.description}), 400


# Error status: Client error.
@app.errorhandler(RequestSchemaValidationError)
def bad_request(e):
    return {"error": str(e.validation_error)}, 400
