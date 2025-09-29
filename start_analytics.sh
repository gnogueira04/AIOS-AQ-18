#!/bin/bash

cd /root/AIOS-QA/age-gender-classification-AQ18

source venv/bin/activate

echo "Starting FFMPEG stream..."

nohup ffmpeg -re -stream_loop -1 -i cam05_video.mp4 -an -c:v libx264 -preset veryfast -tune zerolatency -b:v 2500k -f flv rtmp://147.79.106.102:1935/live/AQ-18 > ffmpeg.log 2>&1 &
echo $! > analytics_ffmpeg.pid

echo "Starting Flask server..."

nohup gunicorn --workers 1 --bind 0.0.0.0:5151 app:app > gunicorn.log 2>&1 &
echo $! > analytics_gunicorn.pid

echo "Service started."
