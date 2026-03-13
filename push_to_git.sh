#!/usr/bin/env bash
set -Eeuo pipefail

REPO_URL="https://github.com/gabrieljonessigmacomputing/lookml_to_dbt_to_sigma.git"
BRANCH="main"
COMMIT_MSG="Integrated relationship mapping and create_lookml.sh"
MAX_PUSH_ATTEMPTS=3
RUN_TS="$(date -u +"%Y%m%dT%H%M%SZ")"

echo "Initializing Git repository safely..."

# Update README when pushing (optional)
if [ -f "./make_readme.sh" ]; then
  echo "Running make_readme.sh..."
  bash ./make_readme.sh
fi

# Initialize only if needed
if [ ! -d .git ]; then
  git init
fi

# Stage everything
git add -A

# Create a commit when needed.
# If this is a brand-new repo with no commits yet, create an initial commit
# even if the tree is empty, so the first push always works.
if git rev-parse --verify HEAD >/dev/null 2>&1; then
  if ! git diff --cached --quiet; then
    git commit -m "$COMMIT_MSG"
  else
    echo "No staged changes detected; reusing existing HEAD."
  fi
else
  git commit --allow-empty -m "$COMMIT_MSG"
fi

# Standardize the branch name
git branch -M "$BRANCH"

# Ensure origin points to the intended repo
if git remote get-url origin >/dev/null 2>&1; then
  git remote set-url origin "$REPO_URL"
else
  git remote add origin "$REPO_URL"
fi

attempt=1
while [ "$attempt" -le "$MAX_PUSH_ATTEMPTS" ]; do
  echo "Push attempt $attempt of $MAX_PUSH_ATTEMPTS..."

  # Does origin/main already exist?
  if git ls-remote --exit-code --heads origin "$BRANCH" >/dev/null 2>&1; then
    # Fetch the latest remote main into the remote-tracking ref
    git fetch --no-tags origin "refs/heads/${BRANCH}:refs/remotes/origin/${BRANCH}"

    REMOTE_SHA="$(git rev-parse "refs/remotes/origin/${BRANCH}")"
    SHORT_SHA="${REMOTE_SHA:0:12}"
    BACKUP_BRANCH="backup/${BRANCH}/${RUN_TS}-attempt${attempt}-${SHORT_SHA}"

    echo "Remote ${BRANCH} currently points to ${REMOTE_SHA}"
    echo "Creating remote backup branch: ${BACKUP_BRANCH}"

    # Preserve the current remote main before overwriting it
    git push origin "refs/remotes/origin/${BRANCH}:refs/heads/${BACKUP_BRANCH}"

    echo "Replacing origin/${BRANCH} with local ${BRANCH} using lease protection..."
    if git push \
      --force-with-lease="refs/heads/${BRANCH}:${REMOTE_SHA}" \
      -u origin \
      "refs/heads/${BRANCH}:refs/heads/${BRANCH}"
    then
      echo "Done! Code is live. Previous remote tip preserved at ${BACKUP_BRANCH}"
      exit 0
    fi

    echo "Lease check failed because origin/${BRANCH} changed after fetch. Retrying..."
  else
    echo "origin/${BRANCH} does not exist yet. Performing initial push..."
    git push -u origin "refs/heads/${BRANCH}:refs/heads/${BRANCH}"
    echo "Done! Code is live."
    exit 0
  fi

  attempt=$((attempt + 1))
done

echo "ERROR: Could not complete push safely after ${MAX_PUSH_ATTEMPTS} attempts because origin/${BRANCH} kept changing." >&2
exit 1