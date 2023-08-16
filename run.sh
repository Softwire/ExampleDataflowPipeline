#!/bin/bash

set -euxo pipefail

mvn -Pdataflow-runner compile exec:java \
    -Dexec.mainClass=com.softwire.examplebeampipeline.DessertIngredients \
    -Dexec.args=" \
    --project=dfeg-396110 \
    --gcpTempLocation=gs://dfeg-396110/temp/ \
    --runner=DataflowRunner \
    --region=us-central1\
"