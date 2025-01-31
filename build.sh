docker build -t reveal-backend:0.0.2 .
docker buildx build --platform linux/arm64 -t reveal-backend:0.0.2_arm64 --load .
