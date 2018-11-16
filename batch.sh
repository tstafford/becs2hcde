#!/bin/bash
# this will submit the job to batch so we don't overload the server when we trigger all these jobs to run at midnight
cat << EOM | batch > /dev/null 2>&1
/opt/queries/HemaConnect_DonorEligibility/updateDonorEligibility.sh
EOM
