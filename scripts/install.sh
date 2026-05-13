#!/usr/bin/env bash
set -euo pipefail

# Delatometry Database node installer for ROS 2 Jazzy on Raspberry Pi / Debian Bookworm.
# Installs OS packages, MariaDB, Python dependencies, creates the default DB/user,
# and optionally rebuilds the database ROS package.
#
# Usage:
#   cd ~/ros2_delatometry
#   bash src/database/scripts/install.sh
#
# Optional overrides:
#   WORKSPACE=$HOME/ros2_delatometry \
#   ROS_SETUP=/opt/ros/jazzy/setup.bash \
#   VENV_DIR=$HOME/venvs/ros2_delatometry_webui \
#   DB_NAME=exp DB_USER=ubuntu DB_PASSWORD=raspberry \
#   bash src/database/scripts/install.sh

WORKSPACE="${WORKSPACE:-$HOME/ros2_delatometry}"
ROS_SETUP="${ROS_SETUP:-/opt/ros/jazzy/setup.bash}"
VENV_DIR="${VENV_DIR:-$HOME/venvs/ros2_delatometry_webui}"
DB_NAME="${DB_NAME:-exp}"
DB_USER="${DB_USER:-ubuntu}"
DB_PASSWORD="${DB_PASSWORD:-raspberry}"
DB_HOST="${DB_HOST:-127.0.0.1}"
DB_PORT="${DB_PORT:-3306}"
BUILD_PACKAGE="${BUILD_PACKAGE:-1}"

log() {
  echo "[database install] $*"
}

source_safely() {
  local file="$1"
  if [ ! -f "$file" ]; then
    echo "ERROR: setup file not found: $file" >&2
    exit 1
  fi
  # ROS 2 setup scripts may reference unset variables; disable nounset while sourcing.
  set +u
  # shellcheck disable=SC1090
  source "$file"
  set -u
}

if [ ! -d "$WORKSPACE" ]; then
  echo "ERROR: workspace not found: $WORKSPACE" >&2
  exit 1
fi

if [ ! -d "$WORKSPACE/src/database" ]; then
  echo "ERROR: database package not found: $WORKSPACE/src/database" >&2
  exit 1
fi

log "workspace: $WORKSPACE"
log "ROS setup:  $ROS_SETUP"
log "venv:       $VENV_DIR"
log "database:   $DB_NAME"
log "db user:    $DB_USER"
log "db host:    $DB_HOST:$DB_PORT"

log "Installing OS packages..."
sudo apt update
sudo apt install -y \
  mariadb-server \
  mariadb-client \
  python3-venv \
  python3-pip \
  python3-dev \
  build-essential

log "Enabling and starting MariaDB..."
sudo systemctl enable mariadb
sudo systemctl start mariadb
sudo systemctl --no-pager --full status mariadb >/dev/null || {
  echo "ERROR: MariaDB service is not healthy" >&2
  sudo systemctl status mariadb --no-pager
  exit 1
}

log "Creating/updating database and user..."
# Uses MariaDB unix_socket root access via sudo.
sudo mysql <<SQL
CREATE DATABASE IF NOT EXISTS \`${DB_NAME}\` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER IF NOT EXISTS '${DB_USER}'@'localhost' IDENTIFIED BY '${DB_PASSWORD}';
CREATE USER IF NOT EXISTS '${DB_USER}'@'127.0.0.1' IDENTIFIED BY '${DB_PASSWORD}';
ALTER USER '${DB_USER}'@'localhost' IDENTIFIED BY '${DB_PASSWORD}';
ALTER USER '${DB_USER}'@'127.0.0.1' IDENTIFIED BY '${DB_PASSWORD}';
GRANT ALL PRIVILEGES ON \`${DB_NAME}\`.* TO '${DB_USER}'@'localhost';
GRANT ALL PRIVILEGES ON \`${DB_NAME}\`.* TO '${DB_USER}'@'127.0.0.1';
FLUSH PRIVILEGES;
SQL

log "Creating/updating Python venv..."
python3 -m venv --system-site-packages "$VENV_DIR"
# shellcheck disable=SC1090
source "$VENV_DIR/bin/activate"

log "Installing Python dependencies..."
python3 -m pip install --upgrade pip setuptools wheel

if [ -f "$WORKSPACE/src/database/requirements.txt" ]; then
  python3 -m pip install -r "$WORKSPACE/src/database/requirements.txt"
else
  python3 -m pip install mysql-connector-python
fi

log "Making database executable wrapper executable..."
if [ -f "$WORKSPACE/src/database/database/run.py" ]; then
  chmod +x "$WORKSPACE/src/database/database/run.py"
else
  echo "ERROR: missing executable wrapper: $WORKSPACE/src/database/database/run.py" >&2
  exit 1
fi

log "Checking ROS environment..."
source_safely "$ROS_SETUP"
cd "$WORKSPACE"

if [ "$BUILD_PACKAGE" = "1" ]; then
  log "Building database package..."
  rm -rf build/database install/database
  colcon build --symlink-install --packages-select database
  source_safely "$WORKSPACE/install/setup.bash"
else
  if [ -f "$WORKSPACE/install/setup.bash" ]; then
    source_safely "$WORKSPACE/install/setup.bash"
  fi
fi

log "Verifying Python imports..."
python3 - <<'PY'
import mysql.connector
from database.srv import Query
from database_node.node_connector import main
print('mysql.connector OK')
print('database.srv.Query OK')
print('database_node.node_connector OK')
PY

log "Verifying MariaDB login..."
python3 - <<PY
import mysql.connector
conn = mysql.connector.connect(
    host='${DB_HOST}',
    port=int('${DB_PORT}'),
    user='${DB_USER}',
    password='${DB_PASSWORD}',
    database='${DB_NAME}',
    connection_timeout=5,
)
cur = conn.cursor()
cur.execute('SELECT 1')
print('MariaDB query OK:', cur.fetchone()[0])
cur.close()
conn.close()
PY

log "Checking ROS executable registration..."
if ros2 pkg executables database | grep -q 'database .*run.py\|database run.py'; then
  ros2 pkg executables database
else
  echo "WARNING: ros2 pkg executables database did not list run.py" >&2
  echo "Check executable bit and CMake install(PROGRAMS database/run.py DESTINATION lib/\${PROJECT_NAME})." >&2
fi

cat <<EOF2

[database install] OK

Start database node manually:
  cd $WORKSPACE
  source $ROS_SETUP
  source $WORKSPACE/install/setup.bash
  source $VENV_DIR/bin/activate
  ros2 launch database db.launch.py

Test service from another terminal:
  cd $WORKSPACE
  source $ROS_SETUP
  source $WORKSPACE/install/setup.bash
  ros2 service list -t | grep database
  ros2 service call /database/query database/srv/Query "{query: '{\"cmd\":\"health\"}'}"

EOF2
