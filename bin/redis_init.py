import redis


# inserting by username and scores
def insert_results_in_redis(username, score):
    r = redis.Redis()
    r.hincrby("users:" + username, "total_score", score)
    r.hincrby("users:" + username, "game_count", 1)
    number_of_games = int(r.hget("users:" + username, "game_count").decode("UTF-8"))
    total_score = int(r.hget("users:" + username, "total_score").decode("UTF-8"))
    avg_score = total_score / number_of_games

    r.zadd("wordle_leaderboard", {"users:" + username: avg_score})


if __name__ == "__main__":
    print("Initializing Redis")
    insert_results_in_redis("dummy", 0)
    insert_results_in_redis("dummy", 3)
    insert_results_in_redis("money", 4)
    insert_results_in_redis("money", 3)
    insert_results_in_redis("monty", 2)
    insert_results_in_redis("sunny", 2)
    insert_results_in_redis("sunny", 1)
    insert_results_in_redis("harshith", 3)
    insert_results_in_redis("jayraj", 4)
    insert_results_in_redis("heet", 5)
    insert_results_in_redis("yash", 2)
    insert_results_in_redis("python", 3)
    insert_results_in_redis("foo", 4)
    insert_results_in_redis("bar", 6)
    print("Initializing of Redis complete")
