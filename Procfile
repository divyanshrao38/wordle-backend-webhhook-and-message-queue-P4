#game: hypercorn game --reload --debug --bind wordle.local.gd:$PORT --access-logfile - --error-logfile - --log-level DEBUG
user: hypercorn user --reload --debug --bind wordle.local.gd:$PORT --access-logfile - --error-logfile - --log-level DEBUG
primary: ./bin/litefs -config ./etc/primary.yml
secondary: ./bin/litefs -config ./etc/secondary.yml
tertiary: ./bin/litefs -config ./etc/tertiary.yml
leaderboard: hypercorn leaderboard --reload --debug --bind wordle.local.gd:$PORT --access-logfile - --error-logfile - --log-level DEBUG
worker: rq worker --verbose
