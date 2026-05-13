#!/usr/bin/env bash

# usage: ./update.sh [branch]

set -e

CURRENT="$(git rev-parse --abbrev-ref HEAD)"

if [[ -z "$1" ]]; then
  if [[ "$CURRENT" == "main" ]]; then
    BRANCH="main"
    echo "▶ Already on main, pulling latest..."
    git pull origin main
  else
    read -rp "Not on main (currently: $CURRENT). Switch to main? [y/N] " confirm
    if [[ "$confirm" =~ ^[Yy]$ ]]; then
      BRANCH="main"
      echo "▶ Fetching latest..."
      git fetch origin
      echo "▶ Switching to branch: $BRANCH"
      git checkout "$BRANCH"
      git pull origin main
    else
      BRANCH="$CURRENT"
      echo "▶ Staying on $CURRENT, fetching latest..."
      git fetch origin
      git pull origin "$CURRENT"
    fi
  fi
else
  BRANCH="$1"
  echo "▶ Fetching latest..."
  git fetch origin
  echo "▶ Switching to branch: $BRANCH"
  git checkout "$BRANCH"
  git pull origin "$BRANCH"
fi

echo "▶ Bringing compose down..."
sudo docker compose down

echo "▶ Building and starting compose..."
sudo docker compose up -d --build

echo "✔ Done — running on branch: $BRANCH"
