#!/bin/bash

# Define variables for account and project.  This makes the script reusable.
ACCOUNT="jayesh.a@faculty.masterschool.com"
PROJECT="mineral-rune-450809-a4"
SERVICE_ACCOUNT="dataengg-project@${PROJECT}.iam.gserviceaccount.com"
ROLE="roles/logging.logWriter"

# Switch to the desired account.
echo "Switching to account: $ACCOUNT"
gcloud config set account "$ACCOUNT"

# Verify that the account was switched successfully.
echo "Verifying active account:"
gcloud config list account

# Add the IAM policy binding.
echo "Adding IAM policy binding for project: $PROJECT"
gcloud projects add-iam-policy-binding "$PROJECT" \
  --member="serviceAccount:$SERVICE_ACCOUNT" \
  --role="$ROLE"

echo "IAM policy binding command executed."
echo "------------------------------------------"
echo "Project: $PROJECT"
echo "Service Account: $SERVICE_ACCOUNT"
echo "Role: $ROLE"
echo "------------------------------------------"
echo "If there were no errors, the operation was likely successful."
