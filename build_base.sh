docker build -t reveal-backend-base:0.0.1 -f Dockerfile.base .
docker buildx build --platform linux/arm64 -t reveal-backend-base:0.0.1_arm64 --load -f Dockerfile.base .
