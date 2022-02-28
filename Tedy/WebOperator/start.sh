#!/usr/bin/env bash

gunicorn -w 40 -b :17043 weboperator:app


# pip install gunicorn
