#!/bin/sh  

pip install -r requirements.txt

# execute the pipeline
echo "Execute the pipeline ..."
python data/pipeline_script.py

# test if pipeline works correct
echo "Test if pipeline works correctly ..."
pytest ../data/test_pipeline.py

