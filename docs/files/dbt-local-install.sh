#!/bin/bash

#######################################
# Resolve script location and root dir
#######################################
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"  # Go up two levels to reach project root

VENV_DIR="$ROOT_DIR/dbt-env"
VSCODE_DIR="$ROOT_DIR/.vscode"

# Example files assumed to live alongside this script in docs/files/
SETTINGS_SOURCE="$SCRIPT_DIR/settings.example.json"
TASKS_SOURCE="$SCRIPT_DIR/tasks.example.json"
PROFILES_EXAMPLE="$SCRIPT_DIR/profiles.example.yml"

ENV_EXAMPLE_SRC="$ROOT_DIR/.env.example"
ENV_FILE="$ROOT_DIR/.env"

ACTIVATE_FILE="$VENV_DIR/bin/activate"

GLOBAL_PROFILES_DIR="$HOME/.dbt"
GLOBAL_PROFILES_FILE="$GLOBAL_PROFILES_DIR/profiles.yml"
PROJECT_PROFILES_SYMLINK="$ROOT_DIR/profiles.yml"

#######################################
# 1. Set up the Python virtual environment
#######################################
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating Python virtual environment in $VENV_DIR ..."
    python3 -m venv "$VENV_DIR"
else
    echo "Virtual environment already exists at $VENV_DIR."
fi

# Activate the environment
# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"

echo "Upgrading pip, wheel, setuptools..."
pip install --upgrade pip wheel setuptools

echo "Installing dbt-core, dbt-snowflake, sqlfluff-templater-dbt..."
pip install --upgrade dbt-core dbt-snowflake sqlfluff-templater-dbt

echo "Pulling dbt dependencies..."
dbt deps

#######################################
# 2. Create .vscode directory if it does not exist
#######################################
if [ ! -d "$VSCODE_DIR" ]; then
    echo "Creating $VSCODE_DIR ..."
    mkdir -p "$VSCODE_DIR"
fi

#######################################
# 3. Copy VS Code settings.json and tasks.json (skip if target exists)
#######################################
TARGET_SETTINGS_JSON="$VSCODE_DIR/settings.json"
if [ -f "$TARGET_SETTINGS_JSON" ]; then
    echo "$TARGET_SETTINGS_JSON already exists. Skipping overwrite."
else
    if [ -f "$SETTINGS_SOURCE" ]; then
        echo "Copying settings.example.json -> $TARGET_SETTINGS_JSON ..."
        cp "$SETTINGS_SOURCE" "$TARGET_SETTINGS_JSON"
    else
        echo "settings.example.json not found in $SCRIPT_DIR. Skipping."
    fi
fi

TARGET_TASKS_JSON="$VSCODE_DIR/tasks.json"
if [ -f "$TARGET_TASKS_JSON" ]; then
    echo "$TARGET_TASKS_JSON already exists. Skipping overwrite."
else
    if [ -f "$TASKS_SOURCE" ]; then
        echo "Copying tasks.example.json -> $TARGET_TASKS_JSON ..."
        cp "$TASKS_SOURCE" "$TARGET_TASKS_JSON"
    else
        echo "tasks.example.json not found in $SCRIPT_DIR. Skipping."
    fi
fi

#######################################
# 4. Ensure ~/.dbt/profiles.yml exists (skip if already present)
#######################################
if [ ! -f "$GLOBAL_PROFILES_FILE" ]; then
    echo "No profiles.yml found at $GLOBAL_PROFILES_FILE."
    if [ ! -d "$GLOBAL_PROFILES_DIR" ]; then
        echo "Creating $GLOBAL_PROFILES_DIR directory..."
        mkdir -p "$GLOBAL_PROFILES_DIR"
    fi
    if [ -f "$PROFILES_EXAMPLE" ]; then
        echo "Copying $PROFILES_EXAMPLE -> $GLOBAL_PROFILES_FILE ..."
        cp "$PROFILES_EXAMPLE" "$GLOBAL_PROFILES_FILE"
        echo "Created ~/.dbt/profiles.yml from example. Update credentials as needed."
    else
        echo "profiles.example.yml not found in $SCRIPT_DIR. Please create ~/.dbt/profiles.yml manually."
    fi
else
    echo "$GLOBAL_PROFILES_FILE already exists. Skipping creation."
fi

#######################################
# 5. Create/verify symlink in root for profiles.yml (skip overwrite if normal file)
#######################################
if [ -L "$PROJECT_PROFILES_SYMLINK" ]; then
    # It's already a symlink; verify it points to ~/.dbt/profiles.yml
    CURRENT_TARGET="$(readlink "$PROJECT_PROFILES_SYMLINK")"
    if [ "$CURRENT_TARGET" = "$GLOBAL_PROFILES_FILE" ]; then
        echo "profiles.yml symlink in root is already correct."
    else
        echo "profiles.yml symlink in root points to a different location. Skipping changes."
        echo "If you want to point it to ~/.dbt/profiles.yml, remove it first."
    fi
elif [ -e "$PROJECT_PROFILES_SYMLINK" ]; then
    # It's a regular file or directory, not a symlink
    echo "profiles.yml in root is not a symlink. Skipping changes to avoid overwriting."
    echo "Remove or rename $PROJECT_PROFILES_SYMLINK if you want a symlink to ~/.dbt/profiles.yml."
else
    # No file or symlink at $ROOT_DIR/profiles.yml
    echo "Creating symlink: $PROJECT_PROFILES_SYMLINK -> $GLOBAL_PROFILES_FILE"
    ln -s "$GLOBAL_PROFILES_FILE" "$PROJECT_PROFILES_SYMLINK"
fi

#######################################
# 6. Update the activate script with extra environment variables (skip if present)
#######################################
if [ -f "$ACTIVATE_FILE" ]; then
    if ! grep -q "DBT_DEFER" "$ACTIVATE_FILE" 2>/dev/null; then
        echo "Updating $ACTIVATE_FILE with DBT_DEFER and DBT_STATE environment variables..."
        cat <<EOL >> "$ACTIVATE_FILE"

export DBT_DEFER="true"
export DBT_STATE="$ROOT_DIR/"
EOL
    else
        echo "Environment variables already set in $ACTIVATE_FILE."
    fi
else
    echo "Virtual env activate file ($ACTIVATE_FILE) not found. Something may be wrong with the venv."
fi

#######################################
# 7. Configure .env for manifest update (skip if .env already exists)
#######################################
if [ -f "$ENV_FILE" ]; then
    echo ".env file already exists in the root. Skipping overwrite."
else
    if [ -f "$ENV_EXAMPLE_SRC" ]; then
        cp "$ENV_EXAMPLE_SRC" "$ENV_FILE"
        echo "Created .env from .env.example. Please fill in DBT_CLOUD_API_TOKEN in $ENV_FILE."
    else
        echo ".env.example not found in $ROOT_DIR. Please create .env manually."
    fi
fi

#######################################
# 8. Final step
#######################################
echo "Setup complete. Restart VS Code to apply changes."
