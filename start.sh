#!/bin/bash
gunicorn app:app.server --workers 4 --bind 0.0.0.0:$PORT