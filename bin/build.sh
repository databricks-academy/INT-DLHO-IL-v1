source ~/.zshrc
# TODO: run the script to build the test zip
bdc build.yaml -o
cp /Users/$USER/tmp/curriculum/delta-lake-hands-on-1.0.0/StudentFiles/Labs.dbc .
mv /Users/$USER/tmp/curriculum/delta-lake-hands-on-1.0.0/StudentFiles/Labs.dbc \
  /Users/$USER/tmp/curriculum/delta-lake-hands-on-1.0.0/Lessons.dbc
databricks workspace delete /Users/$USER@databricks.com/delta-lake-hands-on -r
databricks workspace import Labs.dbc /Users/$USER@databricks.com/delta-lake-hands-on -f DBC -l PYTHON
rm Labs.dbc
