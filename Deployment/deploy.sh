#!/bin/sh

sudo apt-get install python3-venv
python3 -m venv .env
source .env/bin/activate
pip install -r requirements.txt
python3 src/main/java/com/walmart/org/app.py
