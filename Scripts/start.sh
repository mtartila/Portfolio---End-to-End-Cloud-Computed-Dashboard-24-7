#!/bin/bash
gunicorn DashWebComponent:app.server --workers 4 --bind 0.0.0.0:$PORT
