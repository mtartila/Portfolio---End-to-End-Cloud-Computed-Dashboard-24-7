#!/bin/bash
gunicorn Scripts.DashWebComponentV2:server --workers 4 --bind 0.0.0.0:$PORT
