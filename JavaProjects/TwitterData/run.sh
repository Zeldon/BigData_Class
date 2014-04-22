# The working directory, Input data must be in DATA_DIR/input
DATA_DIR=/data/100m

hadoop jar classes.jar sg.edu.nus.cs5344.spring14.twitter.TwMain -d $DATA_DIR --only X
hadoop fs -ls $DATA_DIR/output/