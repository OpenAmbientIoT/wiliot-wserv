#!/bin/bash

# Wiliot Hackathon entry for William Wood Harter
# (c) copyright 2023 - William Wood Harter
#
# License: MIT License

# you can put export commands into the secrets file.
# This should include
# export ALEXA_CLIENT_ID = "xyz"
# export ALEXA_CLIENT_SECRET = "123"
# As specified here
#https://developer.amazon.com/en-US/docs/alexa/smapi/get-access-token-smapi.html#configure-lwa-security-profile

if [ -f ".secrets" ];
then
    . .secrets
fi

# run
if [ "$WSERV_DEBUG" = "TRUE" ];
then
    echo "ENV WSERV_DEBUG==TRUE"
    echo "RUNNING WSERV IN DEBUG MODE - YOU NEED TO START THE PROCESS MANUALLY"
    echo "This container will run for 2M seconds or till you kill it"
    # just run something that will keep the container running
    sleep 2000000
else
    echo "STARTING WSERV watch.py"
    cd /wserv
    python3 watch.py&

    echo "STARTING npm server watch.py"
    cd /wserv/ui
    npm start&

    echo "STARTING WSERV flask"
    cd /wserv/wflask
    flask run --host=0.0.0.0
fi