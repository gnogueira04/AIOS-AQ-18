#!/bin/bash

cd /root/AIOS-QA/age-gender-classification-AQ18

echo "Stopping service..."

if [ -f analytics_gunicorn.pid ]; then
    kill $(cat analytics_gunicorn.pid)
    rm analytics_gunicorn.pid
fi

if [ -f analytics_ffmpeg.pid ]; then
    kill $(cat analytics_ffmpeg.pid)
    rm analytics_ffmpeg.pid
fi

echo "Service stopped."
