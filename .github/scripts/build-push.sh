#!/usr/bin/env bash
set -euo pipefail

AWS_REGION="${AWS_REGION:-us-east-1}"
REPOSITORY="${REPOSITORY:-dora-bond-trading-strategies}"
IMAGE_TAG="${IMAGE_TAG:-$(git rev-parse HEAD)}"
DOCKER_PLATFORM="${DOCKER_PLATFORM:-linux/amd64}"

token_file=""

cleanup() {
	if [[ -n "$token_file" ]]; then
		rm -f "$token_file"
	fi
}
trap cleanup EXIT

github_token() {
	if [[ -n "${GITHUB_TOKEN:-}" ]]; then
		printf '%s' "$GITHUB_TOKEN"
		return
	fi

	if [[ -n "${GH_TOKEN:-}" ]]; then
		printf '%s' "$GH_TOKEN"
		return
	fi

	gh auth token
}

ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
ECR_REGISTRY="${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
IMAGE_URI="${ECR_REGISTRY}/${REPOSITORY}:${IMAGE_TAG}"

if ! aws ecr describe-repositories --repository-names "$REPOSITORY" >/dev/null 2>&1; then
	echo "ECR repository ${REPOSITORY} does not exist. Apply terraform/shared first." >&2
	exit 1
fi

if aws ecr describe-images --repository-name "$REPOSITORY" --image-ids imageTag="$IMAGE_TAG" >/dev/null 2>&1; then
	echo "ECR tag ${REPOSITORY}:${IMAGE_TAG} already exists; skipping build/push."
	echo "IMAGE_URI=${IMAGE_URI}"
	exit 0
fi

aws ecr get-login-password --region "$AWS_REGION" |
	docker login --username AWS --password-stdin "$ECR_REGISTRY" >/dev/null

token_file="$(mktemp /tmp/bond-trading-github-token.XXXXXX)"
github_token >"$token_file"

docker build \
	--platform "$DOCKER_PLATFORM" \
	--secret id=github_token,src="$token_file" \
	-t "$IMAGE_URI" \
	.

docker push "$IMAGE_URI"

echo "IMAGE_URI=${IMAGE_URI}"
