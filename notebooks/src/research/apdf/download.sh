#!/usr/bin/env bash

# Download the APDF datasets to sources/ directory

# ------------------------------------------------------------------------------------------
# config
# ------------------------------------------------------------------------------------------

SOURCE_FOLDER="sources"
RCLONE_REMOTE="rpc"
BUCKET_NAME="api.production-appels-de-fonds"

# ------------------------------------------------------------------------------------------
# user arguments
# ------------------------------------------------------------------------------------------

# Check if the campaign_id and month are provided
if [ -z "$1" ] || [ -z "$2" ]; then
    echo "Usage: ./download.sh <yyyy-mm> <campaign_id>"
    exit 1
fi

# Set the campaign_id and month
MONTH=$1
CAMPAIGN_ID=$2

rclone copy "${RCLONE_REMOTE}:${BUCKET_NAME}/${CAMPAIGN_ID}/" "${SOURCE_FOLDER}/" \
  --include "APDF-${MONTH}*"
