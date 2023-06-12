#!/bin/sh  

pip install -r .testing/requirements.txt

# execute the pipeline
echo "Execute the pipeline ..."
python ./testing/data/pipeline_script.py

# test if pipeline works correct
echo "Test if pipeline works correctly ..."
pytest ./testing/data/test_pipeline.py

