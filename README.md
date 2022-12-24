# CPSC 449 Project 4

[Project 4](https://docs.google.com/document/d/19BqaDN9M9fMfw6WjwISGDauF_I2w20UJ4lNmW9USbn0/edit) involves extending the base Wordle backend application from [Project 1](https://docs.google.com/document/d/14YzD8w5SpJk0DqizgrgyOsXvQ2-rrd-39RUSe2GNvz4/edit) and implementation of nginx to authenticate endpoints and load balancing from [Project 2](https://docs.google.com/document/d/1BXrmgSclvifgYWItGxxhZ72BrmiD5evXoRbA_uRP_jM/edit) and  configuring replication using liteFS and posting the results to the leaderboard service using redis from [Project 3](https://docs.google.com/document/d/1OWltxCFRsd2s4khOdfwKLZ3vqF6dsJ087nyMn0klcQs/edit) . This includes the following objectives:

- Registering the callback url in the game service.
- Sending the scores of the games to the leaderboard service using queuing mechanism.
- Retrying failed jobs using cron scheduler.

This project also builds upon concepts introduced in [Exercise 4](https://docs.google.com/document/d/1GeF5txkEb3Jl0_YtnFKFh21xiDff1IJ54XC9Qydk3GE/edit) which involved setting up a webhook and enqueuing and running jobs using RQ.
### Authors
Section 02
Group 17
Members:
- Jayraj Arora
- Zhiqiang Liu
- Manan Hasmukhbhai Patel
- Divyansh Mohan Rao

## Setting Up
### Development Environment 
Tuffix 2020 (Linux)

### Application Prerequisites
- Python
- Nginx
- Quart
- Sqlite3
- Foreman
- Databases
- Quart-Schema
- Curl
- HTTPie
- Redis
- LiteFS

### VHost Setup
1. Make sure that nginx is running in the background

$ sudo service nginx status

2. Verify that `tuffix-vm` is in `/etc/hosts`

$ cat /etc/hosts

_Note:_ This project uses the hostname `tuffix-vm`. 
3. Go to the project's directory

cd project-name/


4. Copy the VHost file in `/share` to `/etc/nginx/sites-enabled` then restart nginx. It contains the updated configuration for registering the callback url in the games service.

$ sudo cp share/wordle /etc/nginx/sites-enabled/wordle
$ sudo service nginx restart


### Initializing and Starting the Application
1. Remove .keep files added in the application

rm var/primary/mount/.keep
rm var/primary/data/.keep
rm var/secondary/data/.keep
rm var/secondary/mount/.keep
rm var/tertiary/mount/.keep
rm var/tertiary/data/.keep

2. Run three 3 instances of game, 1 instance of user, 1 instance of leaderboard service, and the worker process to run the enqueued jobs

foreman start

3. Run the command below to initialize user and game databases and redis in-memory data store, populate them with dummy values. Please wait as this will take some time

./bin/init.sh

4. Run the command below to add a job that retries the jobs failed if the leaderboard could not run.

crontab requeue_cron_job

NOTE: You can check the list of cronjobs using the below command:

crontab -l





## REST API Features
- Register a user (includes password hashing)
- Authenticate a user (includes hashing verification)
- Start a new game
- Guess a five-letter word
- Retrieve the state of a game in progress
- List the games in progress for a user
- Check the statistics for a particular user
- Register the leaderboard url to the games service at startup of leaderboard service.
- Enqueue jobs after the game has reached a decision(win/loss).
- The job runs using the worker process that post the results of the game to the leaderboard service.
- Retrieve the top 10 users based on their average scores.

## Running the Application

### Registering a User
After starting up the app, create an initial user using the following command with HTTPie where `<username>` and `<password>` are custom values (will need them later when logging in):

http POST http://tuffix-vm/register username=<username> password=<password>

The whole application can only be accessed after authentication, so use the username and password that was created in this step.


### Using Quart-Schema Documentation
The API documentation generated by Quart-Schema for game microservice can be accessed with [this link](http://tuffix-vm/docs) or typing the URL `http://tuffix-vm/docs`. The API documentation for leaderboard microservice can be accessed with [this link](http://127.0.0.1:5400/docs) or typing the URL `http://127.0.0.1:5400/docs`


### Using HTTPie
In order to run via httpie, use the following the commands:
- Creating a user

http POST http://tuffix-vm/register username=<username> password=<password>

- Authenticating a user

http GET http://tuffix-vm/ --auth <username>:<password>

- Creating a game 

http POST http://tuffix-vm/games --auth <username>:<password>

- Checking the state of a game 

http GET http://tuffix-vm/games/<game_id> --auth <username>:<password>

- Playing a certain game/making a guess. This will trigger the enqueue job function that sends the scores to the leaderboard service.

http POST http://tuffix-vm/games/<game_id> guess=<5-letter-word> --auth <username>:<password>

- Retrieving a list of in-progress games

http GET http://tuffix-vm/games/ --auth <username>:<password>
 
- Check the statistics of the user 

http GET http://tuffix-vm/games/statistics --auth <username>:<password>

- The results of the game get posted automatically when the game reaches a decision now.(unless the leaderboard service is down) You can post the results of a game for the leaderboard service manually using the below command.

http POST http://127.0.0.1:5400/results guess_number=<Number from 1 to 6> status=<win or loss> username=<username>

- Retrieve the top 10 users based on their average scores.

http GET http://tuffix-vm/leaderboard

- To test whether the failed jobs ran follow the below steps:
1) Comment the leaderboard service in Procfile. 
2) Restart foreman and try playing a game using [this link](http://tuffix-vm/docs).
3) The job gets failed which can be seen in the redis-cli. 
4) Run the leaderboard service as a separate service using the command: 

hypercorn leaderboard --reload --debug --bind localhost:5400 --access-logfile - --error-logfile - --log-level DEBUG

5) The failed job gets rerun whose output can be seen in the terminal.