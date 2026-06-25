#!/usr/bin/env bash
set -euo pipefail

AWS_REGION="${AWS_REGION:-us-east-1}"
REPOSITORY="${REPOSITORY:-dora-bond-trading-strategies}"
PROJECT_NAME="${PROJECT_NAME:-dora-bond-trading}"
ENVIRONMENT="${ENVIRONMENT:-dev}"
CLUSTER_NAME="${CLUSTER_NAME:-dora-${ENVIRONMENT}}"
STRATEGY_SERVICE_NAME="${STRATEGY_SERVICE_NAME:-${PROJECT_NAME}-strategy-${ENVIRONMENT}}"
PRICE_DAEMON_SERVICE_NAME="${PRICE_DAEMON_SERVICE_NAME:-${PROJECT_NAME}-price-daemon-${ENVIRONMENT}}"
WS_URL="${WS_URL:-wss://${ENVIRONMENT}.dora.co}"
DORA_BASE_URL="${DORA_BASE_URL:-https://${ENVIRONMENT}.dora.co}"
MIGRATE_ONLY="${MIGRATE_ONLY:-false}"

case "$ENVIRONMENT" in
	dev)
		DEFAULT_CORS_ALLOWED_ORIGINS="https://aws-dev.dora.co,https://dev.dora.co,https://dora-awsdev.vercel.app"
		;;
	staging)
		DEFAULT_CORS_ALLOWED_ORIGINS="https://aws-staging.dora.co,https://staging.dora.co"
		;;
	*)
		DEFAULT_CORS_ALLOWED_ORIGINS="https://${ENVIRONMENT}.dora.co"
		;;
esac
CORS_ALLOWED_ORIGINS="${CORS_ALLOWED_ORIGINS:-$DEFAULT_CORS_ALLOWED_ORIGINS}"

require_env() {
	local name="$1"
	if [[ -z "${!name:-}" ]]; then
		echo "${name} is required" >&2
		exit 1
	fi
}

truthy() {
	case "${1:-}" in
		1 | true | TRUE | yes | YES) return 0 ;;
		*) return 1 ;;
	esac
}

put_secret_value() {
	local secret_id="$1"
	local value="$2"

	aws secretsmanager put-secret-value \
		--secret-id "$secret_id" \
		--secret-string "$value" >/dev/null
}

secret_arn() {
	local secret_id="$1"

	aws secretsmanager describe-secret \
		--secret-id "$secret_id" \
		--query ARN \
		--output text
}

require_secret() {
	local secret_id="$1"

	if ! aws secretsmanager describe-secret --secret-id "$secret_id" >/dev/null 2>&1; then
		echo "Secret ${secret_id} does not exist. Apply Terraform for ${ENVIRONMENT} first." >&2
		exit 1
	fi
}

require_service() {
	local service_name="$1"
	local service_count

	service_count="$(
		aws ecs describe-services \
			--cluster "$CLUSTER_NAME" \
			--services "$service_name" \
			--query 'length(services[?status != `INACTIVE`])' \
			--output text
	)"
	if [[ "$service_count" != "1" ]]; then
		echo "ECS service ${service_name} does not exist. Apply Terraform for ${ENVIRONMENT} first." >&2
		exit 1
	fi
}

register_task_definition() {
	local family="$1"
	local cpu="$2"
	local memory="$3"
	local containers="$4"

	aws ecs register-task-definition \
		--family "$family" \
		--requires-compatibilities FARGATE \
		--network-mode awsvpc \
		--cpu "$cpu" \
		--memory "$memory" \
		--execution-role-arn "$EXECUTION_ROLE_ARN" \
		--task-role-arn "$TASK_ROLE_ARN" \
		--runtime-platform cpuArchitecture=X86_64,operatingSystemFamily=LINUX \
		--container-definitions "$containers" \
		--query 'taskDefinition.taskDefinitionArn' \
		--output text
}

run_migration() {
	local task_definition_arn="$1"
	local network_config task_output failures task_arn task_desc exit_code stopped_reason task_id

	network_config="$(
		aws ecs describe-services \
			--cluster "$CLUSTER_NAME" \
			--services "$STRATEGY_SERVICE_NAME" \
			--query 'services[0].networkConfiguration' \
			--output json
	)"

	echo "Running migrations with ${task_definition_arn}"
	task_output="$(
		aws ecs run-task \
			--cluster "$CLUSTER_NAME" \
			--task-definition "$task_definition_arn" \
			--network-configuration "$network_config" \
			--launch-type FARGATE
	)"

	failures="$(jq -r '.failures | length' <<<"$task_output")"
	if [[ "$failures" != "0" ]]; then
		echo "Failed to start migration task:" >&2
		jq '.failures' <<<"$task_output" >&2
		exit 1
	fi

	task_arn="$(jq -r '.tasks[0].taskArn' <<<"$task_output")"
	echo "Migration task: ${task_arn}"

	aws ecs wait tasks-stopped --cluster "$CLUSTER_NAME" --tasks "$task_arn" || true

	task_desc="$(
		aws ecs describe-tasks \
			--cluster "$CLUSTER_NAME" \
			--tasks "$task_arn"
	)"
	exit_code="$(jq -r '[.tasks[0].containers[] | select(.name == "migrate")][0].exitCode // "null"' <<<"$task_desc")"
	stopped_reason="$(jq -r '.tasks[0].stoppedReason // empty' <<<"$task_desc")"

	if [[ "$exit_code" != "0" ]]; then
		echo "Migration failed (exit=${exit_code}, reason=${stopped_reason})" >&2
		task_id="${task_arn##*/}"
		aws logs get-log-events \
			--log-group-name "/ecs/${PROJECT_NAME}-migrate-${ENVIRONMENT}" \
			--log-stream-name "migrate/migrate/${task_id}" \
			--limit 50 \
			--query 'events[*].message' \
			--output text 2>/dev/null || true
		exit 1
	fi

	echo "Migrations completed successfully"
}

require_env IMAGE_TAG

ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
ECR_REGISTRY="${ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
IMAGE_URI="${ECR_REGISTRY}/${REPOSITORY}:${IMAGE_TAG}"
EXECUTION_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${PROJECT_NAME}-${ENVIRONMENT}-ecs-execution-role"
TASK_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${PROJECT_NAME}-${ENVIRONMENT}-ecs-task-role"

DATABASE_URL_SECRET_NAME="${PROJECT_NAME}/${ENVIRONMENT}/database-url"
DORA_API_KEY_SECRET_NAME="${PROJECT_NAME}/${ENVIRONMENT}/dora-api-key"
FRED_API_KEY_SECRET_NAME="${PROJECT_NAME}/${ENVIRONMENT}/fred-api-key"
ENCRYPTION_KEY_SECRET_NAME="${PROJECT_NAME}/${ENVIRONMENT}/encryption-key"

require_service "$STRATEGY_SERVICE_NAME"
require_secret "$DATABASE_URL_SECRET_NAME"
require_secret "$DORA_API_KEY_SECRET_NAME"
require_secret "$FRED_API_KEY_SECRET_NAME"
require_secret "$ENCRYPTION_KEY_SECRET_NAME"

if ! truthy "$MIGRATE_ONLY"; then
	require_env DORA_API_KEY
	require_env ENCRYPTION_KEY
	require_service "$PRICE_DAEMON_SERVICE_NAME"

	put_secret_value "$DORA_API_KEY_SECRET_NAME" "$DORA_API_KEY"
	put_secret_value "$FRED_API_KEY_SECRET_NAME" "${FRED_API_KEY:-not-configured}"
	put_secret_value "$ENCRYPTION_KEY_SECRET_NAME" "$ENCRYPTION_KEY"
fi

DATABASE_URL_SECRET_ARN="$(secret_arn "$DATABASE_URL_SECRET_NAME")"
DORA_API_KEY_SECRET_ARN="$(secret_arn "$DORA_API_KEY_SECRET_NAME")"
FRED_API_KEY_SECRET_ARN="$(secret_arn "$FRED_API_KEY_SECRET_NAME")"
ENCRYPTION_KEY_SECRET_ARN="$(secret_arn "$ENCRYPTION_KEY_SECRET_NAME")"

strategy_containers="$(
	jq -cn \
		--arg image "$IMAGE_URI" \
		--arg db "$DATABASE_URL_SECRET_ARN" \
		--arg dora "$DORA_API_KEY_SECRET_ARN" \
		--arg fred "$FRED_API_KEY_SECRET_ARN" \
		--arg encryption "$ENCRYPTION_KEY_SECRET_ARN" \
		--arg ws_url "$WS_URL" \
		--arg dora_base_url "$DORA_BASE_URL" \
		--arg cors_allowed_origins "$CORS_ALLOWED_ORIGINS" \
		--arg environment "$ENVIRONMENT" \
		'[{
			name: "strategy-server",
			image: $image,
			essential: true,
			entryPoint: ["/app/strategy-server"],
			command: ["--addr", ":8081"],
			readonlyRootFilesystem: false,
			stopTimeout: 10,
			portMappings: [{containerPort: 8081, protocol: "tcp"}],
			environment: [
				{name: "ADDR", value: ":8081"},
				{name: "WS_URL", value: $ws_url},
				{name: "DORA_BASE_URL", value: $dora_base_url},
				{name: "LOG_LEVEL", value: "INFO"},
				{name: "CORS_ALLOWED_ORIGINS", value: $cors_allowed_origins}
			],
			secrets: [
				{name: "DATABASE_URL", valueFrom: $db},
				{name: "API_KEY", valueFrom: $dora},
				{name: "FRED_API_KEY", valueFrom: $fred},
				{name: "ENCRYPTION_KEY", valueFrom: $encryption}
			],
			healthCheck: {
				command: ["CMD-SHELL", "wget -q --spider http://localhost:8081/healthz || exit 1"],
				interval: 30,
				timeout: 5,
				retries: 3,
				startPeriod: 30
			},
			logConfiguration: {
				logDriver: "awslogs",
				options: {
					"awslogs-group": "/ecs/dora-bond-trading-strategy-\($environment)",
					"awslogs-region": "us-east-1",
					"awslogs-stream-prefix": "ecs"
				}
			}
		}]'
)"

price_daemon_containers="$(
	jq -cn \
		--arg image "$IMAGE_URI" \
		--arg db "$DATABASE_URL_SECRET_ARN" \
		--arg dora "$DORA_API_KEY_SECRET_ARN" \
		--arg ws_url "$WS_URL" \
		--arg dora_base_url "$DORA_BASE_URL" \
		--arg environment "$ENVIRONMENT" \
		'[{
			name: "price-daemon",
			image: $image,
			essential: true,
			entryPoint: ["/app/price-daemon"],
			command: ["--http-addr", ":8080"],
			readonlyRootFilesystem: false,
			stopTimeout: 10,
			environment: [
				{name: "HTTP_ADDR", value: ":8080"},
				{name: "WS_URL", value: $ws_url},
				{name: "DORA_BASE_URL", value: $dora_base_url},
				{name: "LOG_LEVEL", value: "INFO"},
				{name: "HEALTH_STALE_AFTER", value: "2m"},
				{name: "HEALTH_STARTUP_GRACE", value: "2m"}
			],
			secrets: [
				{name: "DATABASE_URL", valueFrom: $db},
				{name: "DORA_API_KEY", valueFrom: $dora}
			],
			healthCheck: {
				command: ["CMD-SHELL", "wget -q --spider http://localhost:8080/healthz || exit 1"],
				interval: 30,
				timeout: 5,
				retries: 3,
				startPeriod: 120
			},
			logConfiguration: {
				logDriver: "awslogs",
				options: {
					"awslogs-group": "/ecs/dora-bond-trading-price-daemon-\($environment)",
					"awslogs-region": "us-east-1",
					"awslogs-stream-prefix": "ecs"
				}
			}
		}]'
)"

migrate_containers="$(
	jq -cn \
		--arg image "$IMAGE_URI" \
		--arg db "$DATABASE_URL_SECRET_ARN" \
		--arg environment "$ENVIRONMENT" \
		'[{
			name: "migrate",
			image: $image,
			essential: true,
			entryPoint: ["sh", "-c"],
			command: ["exec /app/tern migrate --conn-string \"$DATABASE_URL\" --migrations /app/migrations"],
			readonlyRootFilesystem: false,
			secrets: [
				{name: "DATABASE_URL", valueFrom: $db}
			],
			logConfiguration: {
				logDriver: "awslogs",
				options: {
					"awslogs-group": "/ecs/dora-bond-trading-migrate-\($environment)",
					"awslogs-region": "us-east-1",
					"awslogs-stream-prefix": "migrate"
				}
			}
		}]'
)"

migration_task_definition="$(register_task_definition "${PROJECT_NAME}-migrate-${ENVIRONMENT}" 256 512 "$migrate_containers")"
run_migration "$migration_task_definition"

if truthy "$MIGRATE_ONLY"; then
	echo "Skipping service deployment because MIGRATE_ONLY=${MIGRATE_ONLY}"
	exit 0
fi

strategy_task_definition="$(register_task_definition "${PROJECT_NAME}-strategy-${ENVIRONMENT}" 1024 2048 "$strategy_containers")"
price_task_definition="$(register_task_definition "${PROJECT_NAME}-price-daemon-${ENVIRONMENT}" 512 1024 "$price_daemon_containers")"

aws ecs update-service \
	--cluster "$CLUSTER_NAME" \
	--service "$STRATEGY_SERVICE_NAME" \
	--task-definition "$strategy_task_definition" \
	--desired-count 1 >/dev/null

aws ecs update-service \
	--cluster "$CLUSTER_NAME" \
	--service "$PRICE_DAEMON_SERVICE_NAME" \
	--task-definition "$price_task_definition" \
	--desired-count 1 >/dev/null

aws ecs wait services-stable \
	--cluster "$CLUSTER_NAME" \
	--services "$STRATEGY_SERVICE_NAME" "$PRICE_DAEMON_SERVICE_NAME"

echo "Deployed ${IMAGE_URI}"
