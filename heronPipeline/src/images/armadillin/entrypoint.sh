



SEQ_BATCH_FILE=$SEQ_DATA_ROOT/$DATE_PARTITION/seqBatchFiles/sequences_$ITERATION_UUID.fasta
gzip -c $SEQ_BATCH_FILE > /tmp/seqFile.gz
armadillin --update
armadillin --seqs_are_aligned /tmp/seqFile.gz > /tmp/results.tsv
head /tmp/results.tsv
python3 app.py