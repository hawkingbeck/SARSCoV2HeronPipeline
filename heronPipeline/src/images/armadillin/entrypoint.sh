


gzip -c $SEQ_BATCH_FILE > /tmp/seqFile.gz
armadillin /tmp/seqFile.gz > /tmp/results.tsv
python3 app.py