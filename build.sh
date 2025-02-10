#!/bin/bash
image=192.168.68.55:5000/reveal-backend:0.0.8
find . -type d -name __pycache__ -exec rm -rf {} \; 
# docker build -t ${image}
docker buildx build --platform linux/arm64 -t ${image}_arm64 --load .
docker push ${image}_arm64
