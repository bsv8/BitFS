#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

if [[ "${PR_AGENT_GATE_DISABLE:-0}" == "1" ]]; then
  echo "pr-agent gate: disabled by PR_AGENT_GATE_DISABLE=1"
  exit 0
fi

if git diff --cached --quiet; then
  echo "pr-agent gate: no staged changes, skip"
  exit 0
fi

if ! command -v uv >/dev/null 2>&1; then
  echo "pr-agent gate: uv is required but not found. commit blocked."
  exit 1
fi

mkdir -p "$repo_root/.pr-agent"

if [[ -f "$repo_root/.secrets/pr-agent.env" ]]; then
  # shellcheck disable=SC1091
  set -a && source "$repo_root/.secrets/pr-agent.env" && set +a
fi

if [[ -z "${ANTHROPIC_KEY:-}" ]]; then
  echo "pr-agent gate: missing ANTHROPIC_KEY (set it in .secrets/pr-agent.env). commit blocked."
  exit 1
fi

base_branch="${PR_AGENT_BASE_BRANCH:-}"
if [[ -z "$base_branch" ]]; then
  default_remote_base="$(git symbolic-ref --quiet --short refs/remotes/origin/HEAD 2>/dev/null || true)"
  if [[ -n "$default_remote_base" ]]; then
    base_branch="${default_remote_base#origin/}"
  elif git show-ref --verify --quiet refs/heads/main; then
    base_branch="main"
  elif git show-ref --verify --quiet refs/heads/master; then
    base_branch="master"
  else
    echo "pr-agent gate: cannot determine base branch. set PR_AGENT_BASE_BRANCH in .secrets/pr-agent.env. commit blocked."
    exit 1
  fi
fi

model="${ANTHROPIC_MODEL:-MiniMax-M2.7}"
if [[ "$model" != anthropic/* ]]; then
  model="anthropic/$model"
fi

tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/pr-agent-gate.XXXXXX")"
cleanup() {
  rm -rf "$tmp_dir"
}
trap cleanup EXIT

patch_file="$tmp_dir/staged.patch"
git diff --cached --binary > "$patch_file"

tmp_repo="$tmp_dir/repo"
git clone --quiet "$repo_root" "$tmp_repo"
cd "$tmp_repo"

git checkout -q -B pr-agent-gate-work HEAD
if ! git apply --index "$patch_file"; then
  echo "pr-agent gate: failed to apply staged patch in temp repo. commit blocked."
  exit 1
fi

if git diff --cached --quiet; then
  echo "pr-agent gate: no effective staged diff after apply, skip"
  exit 0
fi

git -c user.name="pr-agent-gate" -c user.email="pr-agent-gate@local" \
  commit -q -m "temp: pr-agent gate"

review_file="$repo_root/.pr-agent/review.md"
log_file="$repo_root/.pr-agent/run.log"

extra_instructions_default=$'请只审查当前增量代码，重点检查以下规则：\n1. 禁止新增 `*Runtime` 作为函数参数。\n2. 禁止新增对 `runIn` 的直接访问。\n3. 业务目录中禁止新增 `*sql.DB` 作为函数参数。\n4. 若同一个非基础数据类型在本次改动中被多个函数签名复用，请提示“可能形成新的聚合参数”。\n\n输出要求：按“阻断/建议”分级，并给出文件与行号。'
extra_instructions="${PR_AGENT_EXTRA_INSTRUCTIONS:-$extra_instructions_default}"

export CONFIG__GIT_PROVIDER="local"
export CONFIG__PUBLISH_OUTPUT="true"
export CONFIG__PUBLISH_OUTPUT_PROGRESS="false"
export CONFIG__MODEL="$model"
export CONFIG__FALLBACK_MODELS="[\"$model\"]"
export CONFIG__CUSTOM_MODEL_MAX_TOKENS="${PR_AGENT_CUSTOM_MODEL_MAX_TOKENS:-128000}"
export CONFIG__AI_TIMEOUT="${PR_AGENT_AI_TIMEOUT:-300}"
export CONFIG__RESPONSE_LANGUAGE="${PR_AGENT_RESPONSE_LANGUAGE:-zh-CN}"
export LOCAL__REVIEW_PATH="$review_file"
export PR_REVIEWER__NUM_MAX_FINDINGS="${PR_AGENT_NUM_MAX_FINDINGS:-8}"
export PR_REVIEWER__REQUIRE_TESTS_REVIEW="true"
export PR_REVIEWER__REQUIRE_SECURITY_REVIEW="true"
export PR_REVIEWER__EXTRA_INSTRUCTIONS="$extra_instructions"
export ANTHROPIC__KEY="$ANTHROPIC_KEY"
export ANTHROPIC_BASE_URL="${ANTHROPIC_BASE_URL:-https://api.minimaxi.com/anthropic}"

echo "pr-agent gate: running review against base branch '$base_branch'..."
pr_agent_uvx_from="${PR_AGENT_UVX_FROM:-git+https://github.com/qodo-ai/pr-agent@d82f7d3e696cd00822694aaa3096265d3889f3f1}"
pr_agent_python="${PR_AGENT_PYTHON:-3.12}"
if ! uvx --python "$pr_agent_python" --from "$pr_agent_uvx_from" pr-agent --pr_url "$base_branch" review >"$log_file" 2>&1; then
  echo "pr-agent gate: execution failed. see $log_file. commit blocked."
  exit 1
fi

if [[ ! -s "$review_file" ]]; then
  echo "pr-agent gate: no review output generated. see $log_file. commit blocked."
  exit 1
fi

if [[ "${PR_AGENT_GATE_ALLOW_FINDINGS:-0}" == "1" ]]; then
  echo "pr-agent gate: findings allowed by PR_AGENT_GATE_ALLOW_FINDINGS=1"
  exit 0
fi

if grep -qi "No major issues detected" "$review_file"; then
  echo "pr-agent gate: pass"
  exit 0
fi

echo "pr-agent gate: findings detected. commit blocked."
echo "review: $review_file"
echo "log:    $log_file"
exit 1
