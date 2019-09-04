#!/bin/bash +xe

source export_env_variables.sh

aws redshift create-cluster \
--cluster-identifier redshift-cluster \
--db-name dev \
--cluster-type single-node \
--master-username $REDSHIFT_USERNAME \
--master-user-password $REDSHIFT_PASSWORD \
--node-type dc2.large \
--vpc-security-group-ids $REDSHIFT_SECURITY_GROUP \
--iam-roles $REDSHIFT_IAM_ROLE
