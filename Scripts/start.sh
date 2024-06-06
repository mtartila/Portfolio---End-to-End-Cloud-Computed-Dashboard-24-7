#!/bin/bash
gunicorn Scripts.DashWebComponent:server --workers 4 --bind 0.0.0.0:$PORT
