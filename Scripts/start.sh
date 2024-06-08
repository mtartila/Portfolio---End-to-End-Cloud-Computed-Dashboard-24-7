#!/bin/bash
gunicorn Scripts.DashWebComponentV3:server --workers 4 --bind 0.0.0.0:$PORT
