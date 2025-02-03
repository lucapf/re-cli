#!/bin/bash
image=192.168.68.55:5000/reveal-backend:0.0.5

docker build -t ${image}
docker buildx build --platform linux/arm64 -t ${image}_arm64 --load .
docker push ${image}_arm64
